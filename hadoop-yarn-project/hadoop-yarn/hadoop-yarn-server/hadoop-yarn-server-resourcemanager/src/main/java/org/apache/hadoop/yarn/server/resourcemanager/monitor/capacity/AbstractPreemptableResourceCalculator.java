/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Calculate how much resources need to be preempted for each queue,
 * will be used by {@link PreemptionCandidatesSelector}.
 */
public class AbstractPreemptableResourceCalculator {

  protected final CapacitySchedulerPreemptionContext context;
  protected final ResourceCalculator rc;
  private boolean isReservedPreemptionCandidatesSelector;

  static class TQComparator implements Comparator<TempQueuePerPartition> {
    private ResourceCalculator rc;
    private Resource clusterRes;

    TQComparator(ResourceCalculator rc, Resource clusterRes) {
      this.rc = rc;
      this.clusterRes = clusterRes;
    }

    @Override
    public int compare(TempQueuePerPartition tq1, TempQueuePerPartition tq2) {
      if (getIdealPctOfGuaranteed(tq1) < getIdealPctOfGuaranteed(tq2)) {
        return -1;
      }
      if (getIdealPctOfGuaranteed(tq1) > getIdealPctOfGuaranteed(tq2)) {
        return 1;
      }
      return 0;
    }

    // Calculates idealAssigned / guaranteed
    // TempQueues with 0 guarantees are always considered the most over
    // capacity and therefore considered last for resources.
    private double getIdealPctOfGuaranteed(TempQueuePerPartition q) {
      double pctOver = Integer.MAX_VALUE;
      if (q != null && Resources.greaterThan(rc, clusterRes, q.getGuaranteed(),
          Resources.none())) {
        pctOver = Resources.divide(rc, clusterRes, q.idealAssigned,
            q.getGuaranteed());
      }
      return (pctOver);
    }
  }

  /**
   * PreemptableResourceCalculator constructor.
   *
   * @param preemptionContext context
   * @param isReservedPreemptionCandidatesSelector
   *          this will be set by different implementation of candidate
   *          selectors, please refer to TempQueuePerPartition#offer for
   *          details.
   */
  public AbstractPreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector) {
    context = preemptionContext;
    rc = preemptionContext.getResourceCalculator();
    this.isReservedPreemptionCandidatesSelector =
        isReservedPreemptionCandidatesSelector;
  }

  /**
   * Given a set of queues compute the fix-point distribution of unassigned
   * resources among them. As pending request of a queue are exhausted, the
   * queue is removed from the set and remaining capacity redistributed among
   * remaining queues. The distribution is weighted based on guaranteed
   * capacity, unless asked to ignoreGuarantee, in which case resources are
   * distributed uniformly.
   *
   * @param totGuarant
   *          total guaranteed resource
   * @param qAlloc
   *          List of child queues
   * @param unassigned
   *          Unassigned resource per queue
   * @param ignoreGuarantee
   *          ignore guarantee per queue.
   */
  // 按一定规则将资源分给各个队列
  // 1. 首先保障每个队列有自己配置的资源。若使用量小于配置量，多余的资源会被分配到其他队列
  // 2. 若队列有超出配置资源需求，则放到一个优先级队列中，按 (使用量 / 配置量) 从小到大排序
  // 3. 对于有资源需求的队列，在剩余的资源中，按配置比例计算每个队列可分配的资源量
  // 4. 每次从优先级队列中选需求优先级最高的，进行分配
  // 5. 计算 min(可分配量, 队列最大剩余用量, 需求量)。作为本次分配的资源。若仍有资源需求则放回优先级队列，等待下次分配
  // 6. 当满足所有队列资源需求，或者没有剩余资源时结束
  // 7. 仍有资源需求的队列会记录在 underServedQueues
  protected void computeFixpointAllocation(Resource totGuarant,
      Collection<TempQueuePerPartition> qAlloc, Resource unassigned,
      boolean ignoreGuarantee) {
    // 传进来 unassigned = totGuarant
    // Prior to assigning the unused resources, process each queue as follows:
    // If current > guaranteed, idealAssigned = guaranteed + untouchable extra
    // Else idealAssigned = current;
    // Subtract idealAssigned resources from unassigned.
    // If the queue has all of its needs met (that is, if
    // idealAssigned >= current + pending), remove it from consideration.
    // Sort queues from most under-guaranteed to most over-guaranteed.
    // 有序队列，(使用量 / 配置量) 从小到大排序
    TQComparator tqComparator = new TQComparator(rc, totGuarant);
    PriorityQueue<TempQueuePerPartition> orderedByNeed = new PriorityQueue<>(10,
        tqComparator);
    for (Iterator<TempQueuePerPartition> i = qAlloc.iterator(); i.hasNext();) {
      TempQueuePerPartition q = i.next();
      Resource used = q.getUsed();

      // idealAssigned = min(使用量，配置量)。  对于不可抢占队列，则再加上超出的部分，防止资源被再分配。
      if (Resources.greaterThan(rc, totGuarant, used, q.getGuaranteed())) {
        q.idealAssigned = Resources.add(q.getGuaranteed(), q.untouchableExtra);
      } else {
        q.idealAssigned = Resources.clone(used);
      }
      Resources.subtractFrom(unassigned, q.idealAssigned);
      // If idealAssigned < (allocated + used + pending), q needs more
      // resources, so
      // add it to the list of underserved queues, ordered by need.
      Resource curPlusPend = Resources.add(q.getUsed(), q.pending);
      // 如果该队列有超出配置资源需求，就把这个队列放到 orderedByNeed 有序队列中（即这个队列有资源缺口）
      if (Resources.lessThan(rc, totGuarant, q.idealAssigned, curPlusPend)) {
        orderedByNeed.add(q);
      }
    }

    // 此时 unassigned 是 整体可用资源 排除掉 所有已使用的资源（used）
    // assign all cluster resources until no more demand, or no resources are
    // left
    // 把未分配的资源（unassigned）分配出去
    // 方式就是从 orderedByNeed 中每次取出 most under-guaranteed 队列，按规则分配一块资源给他，如果仍不满足就按顺序再放回 orderedByNeed
    // 直到满足所有队列资源，或者没有资源可分配
    while (!orderedByNeed.isEmpty() && Resources.greaterThan(rc, totGuarant,
        unassigned, Resources.none())) {
      Resource wQassigned = Resource.newInstance(0, 0);
      // we compute normalizedGuarantees capacity based on currently active
      // queues
      // 对于有资源缺口的队列，重新计算他们的资源保证比例：normalizedGuarantee。
      // 即 （该队列保证量 / 所有资源缺口队列保证量）
      resetCapacity(unassigned, orderedByNeed, ignoreGuarantee);

      // For each underserved queue (or set of queues if multiple are equally
      // underserved), offer its share of the unassigned resources based on its
      // normalized guarantee. After the offer, if the queue is not satisfied,
      // place it back in the ordered list of queues, recalculating its place
      // in the order of most under-guaranteed to most over-guaranteed. In this
      // way, the most underserved queue(s) are always given resources first.
      // 这里返回是个列表，是因为可能有需求度（优先级）相等的情况
      Collection<TempQueuePerPartition> underserved = getMostUnderservedQueues(
          orderedByNeed, tqComparator);
      for (Iterator<TempQueuePerPartition> i = underserved.iterator(); i
          .hasNext();) {
        TempQueuePerPartition sub = i.next();
        // 按照 normalizedGuarantee 比例能从剩余资源中分走多少。
        Resource wQavail = Resources.multiplyAndNormalizeUp(rc, unassigned,
            sub.normalizedGuarantee, Resource.newInstance(1, 1));
        // 【重点】按一定规则将资源分配给队列，并返回剩下的资源。
        Resource wQidle = sub.offer(wQavail, rc, totGuarant,
            isReservedPreemptionCandidatesSelector);
        // 分配给队列的资源
        Resource wQdone = Resources.subtract(wQavail, wQidle);

        // 这里 wQdone > 0 证明本次迭代分配出去了资源，那么还会放回到待分配资源的集合中（哪怕本次已满足资源请求），直到未再分配资源了才退出。
        if (Resources.greaterThan(rc, totGuarant, wQdone, Resources.none())) {
          // The queue is still asking for more. Put it back in the priority
          // queue, recalculating its order based on need.
          orderedByNeed.add(sub);
        }
        Resources.addTo(wQassigned, wQdone);
      }
      Resources.subtractFrom(unassigned, wQassigned);
    }

    // Sometimes its possible that, all queues are properly served. So intra
    // queue preemption will not try for any preemption. How ever there are
    // chances that within a queue, there are some imbalances. Hence make sure
    // all queues are added to list.
    // 这里有可能整个资源都分配完了，还有队列资源不满足
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      context.addPartitionToUnderServedQueues(q1.queueName, q1.partition);
    }
  }

  /**
   * Computes a normalizedGuaranteed capacity based on active queues.
   *
   * @param clusterResource
   *          the total amount of resources in the cluster
   * @param queues
   *          the list of queues to consider
   * @param ignoreGuar
   *          ignore guarantee.
   */
  private void resetCapacity(Resource clusterResource,
      Collection<TempQueuePerPartition> queues, boolean ignoreGuar) {
    Resource activeCap = Resource.newInstance(0, 0);

    if (ignoreGuar) {
      for (TempQueuePerPartition q : queues) {
        q.normalizedGuarantee = 1.0f / queues.size();
      }
    } else {
      for (TempQueuePerPartition q : queues) {
        Resources.addTo(activeCap, q.getGuaranteed());
      }
      for (TempQueuePerPartition q : queues) {
        q.normalizedGuarantee = Resources.divide(rc, clusterResource,
            q.getGuaranteed(), activeCap);
      }
    }
  }

  // Take the most underserved TempQueue (the one on the head). Collect and
  // return the list of all queues that have the same idealAssigned
  // percentage of guaranteed.
  private Collection<TempQueuePerPartition> getMostUnderservedQueues(
      PriorityQueue<TempQueuePerPartition> orderedByNeed,
      TQComparator tqComparator) {
    ArrayList<TempQueuePerPartition> underserved = new ArrayList<>();
    while (!orderedByNeed.isEmpty()) {
      TempQueuePerPartition q1 = orderedByNeed.remove();
      underserved.add(q1);

      // Add underserved queues in order for later uses
      context.addPartitionToUnderServedQueues(q1.queueName, q1.partition);
      TempQueuePerPartition q2 = orderedByNeed.peek();
      // q1's pct of guaranteed won't be larger than q2's. If it's less, then
      // return what has already been collected. Otherwise, q1's pct of
      // guaranteed == that of q2, so add q2 to underserved list during the
      // next pass.
      if (q2 == null || tqComparator.compare(q1, q2) < 0) {
        if (null != q2) {
          context.addPartitionToUnderServedQueues(q2.queueName, q2.partition);
        }
        return underserved;
      }
    }
    return underserved;
  }
}