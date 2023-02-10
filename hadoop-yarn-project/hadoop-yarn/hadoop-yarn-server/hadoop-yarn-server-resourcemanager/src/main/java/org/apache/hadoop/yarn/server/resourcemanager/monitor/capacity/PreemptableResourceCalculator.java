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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Calculate how much resources need to be preempted for each queue,
 * will be used by {@link PreemptionCandidatesSelector}
 */
public class PreemptableResourceCalculator
    extends
      AbstractPreemptableResourceCalculator {
  private static final Log LOG =
      LogFactory.getLog(PreemptableResourceCalculator.class);

  private boolean isReservedPreemptionCandidatesSelector;

  /**
   * PreemptableResourceCalculator constructor
   *
   * @param preemptionContext preemption policy context
   * @param isReservedPreemptionCandidatesSelector this will be set by
   * different implementation of candidate selectors, please refer to
   * TempQueuePerPartition#offer for details.
   */
  public PreemptableResourceCalculator(
      CapacitySchedulerPreemptionContext preemptionContext,
      boolean isReservedPreemptionCandidatesSelector) {
    super(preemptionContext, isReservedPreemptionCandidatesSelector);
  }

  /**
   * This method computes (for a single level in the tree, passed as a {@code
   * List<TempQueue>}) the ideal assignment of resources. This is done
   * recursively to allocate capacity fairly across all queues with pending
   * demands. It terminates when no resources are left to assign, or when all
   * demand is satisfied.
   *
   * @param rc resource calculator
   * @param queues a list of cloned queues to be assigned capacity to (this is
   * an out param)
   * @param totalPreemptionAllowed total amount of preemption we allow
   * @param tot_guarant the amount of capacity assigned to this pool of queues
   */
  // 按照各队列用量和需求量，重新计算各队列资源分配
  // 超出重分配量的资源就是要被抢占的资源量
  private void computeIdealResourceDistribution(ResourceCalculator rc,
      List<TempQueuePerPartition> queues, Resource totalPreemptionAllowed,
      Resource tot_guarant) {

    // qAlloc tracks currently active queues (will decrease progressively as
    // demand is met)
    List<TempQueuePerPartition> qAlloc = new ArrayList<>(queues);
    // unassigned tracks how much resources are still to assign, initialized
    // with the total capacity for this set of queues
    // 当前子队列所在父队列能够被保证的资源量（absCapacity），所谓资源抢占只是重新分配各子队列最低资源保证
    Resource unassigned = Resources.clone(tot_guarant);

    // group queues based on whether they have non-zero guaranteed capacity
    Set<TempQueuePerPartition> nonZeroGuarQueues = new HashSet<>();
    Set<TempQueuePerPartition> zeroGuarQueues = new HashSet<>();

    // 选出队列配置量不是 0 的；队列配置为 0 的，可能是共享资源池的配置
    for (TempQueuePerPartition q : qAlloc) {
      if (Resources.greaterThan(rc, tot_guarant,
          q.getGuaranteed(), Resources.none())) {
        nonZeroGuarQueues.add(q);
      } else {
        zeroGuarQueues.add(q);
      }
    }

    // first compute the allocation as a fixpoint based on guaranteed capacity
    // （重点）计算并设置各子队列 idealAssigned。把剩余的资源分配给各队列，还有资源不满足的队列就放到 underServedQueues 中
    computeFixpointAllocation(tot_guarant, nonZeroGuarQueues, unassigned,
        false);

    // if any capacity is left unassigned, distributed among zero-guarantee
    // queues uniformly (i.e., not based on guaranteed capacity, as this is zero)
    if (!zeroGuarQueues.isEmpty()
        && Resources.greaterThan(rc, tot_guarant, unassigned, Resources.none())) {
      computeFixpointAllocation(tot_guarant, zeroGuarQueues, unassigned,
          true);
    }

    // based on ideal assignment computed above and current assignment we derive
    // how much preemption is required overall
    // 计算总共要 Preempt 资源
    // (重新分配的资源 - 已使用的资源) 就是要被抢占的资源
    Resource totPreemptionNeeded = Resource.newInstance(0, 0);
    for (TempQueuePerPartition t:queues) {
      if (Resources.greaterThan(rc, tot_guarant,
          t.getUsed(), t.idealAssigned)) {
        Resources.addTo(totPreemptionNeeded, Resources
            .subtract(t.getUsed(), t.idealAssigned));
      }
    }

    /**
     * if we need to preempt more than is allowed, compute a factor (0<f<1)
     * that is used to scale down how much we ask back from each queue
     */
    // 平滑抢占，不是一下把超用的资源都干掉，每次只处理一部分。
    float scalingFactor = 1.0F;
    if (Resources.greaterThan(rc,
        tot_guarant, totPreemptionNeeded, totalPreemptionAllowed)) {
      scalingFactor = Resources.divide(rc, tot_guarant, totalPreemptionAllowed,
          totPreemptionNeeded);
    }

    // assign to each queue the amount of actual preemption based on local
    // information of ideal preemption and scaling factor
    // 计算每个队列计划要被 Preempt 量，即 toBePreempted
    for (TempQueuePerPartition t : queues) {
      t.assignPreemption(scalingFactor, rc, tot_guarant);
    }
  }

  /**
   * This method recursively computes the ideal assignment of resources to each
   * level of the hierarchy. This ensures that leafs that are over-capacity but
   * with parents within capacity will not be preemptionCandidates. Preemptions are allowed
   * within each subtree according to local over/under capacity.
   *
   * @param root the root of the cloned queue hierachy
   * @param totalPreemptionAllowed maximum amount of preemption allowed
   * @return a list of leaf queues updated with preemption targets
   */
  // 递归 重新计算各队列资源分配，同时给每个队列计算出了要 Preempt 量
  private void recursivelyComputeIdealAssignment(
      TempQueuePerPartition root, Resource totalPreemptionAllowed) {
    if (root.getChildren() != null &&
        root.getChildren().size() > 0) {
      // compute ideal distribution at this level
      // root.idealAssigned 是 (totalPartitionResource * root.absCapacity)  也就是当前父队列被保证的资源
      // 重新计算各队列资源分配，同时给每个队列计算出了要 Preempt 量
      computeIdealResourceDistribution(rc, root.getChildren(),
          totalPreemptionAllowed, root.idealAssigned);
      // compute recursively for lower levels and build list of leafs
      for (TempQueuePerPartition t : root.getChildren()) {
        recursivelyComputeIdealAssignment(t, totalPreemptionAllowed);
      }
    }
  }

  // 计算每个队列 preempt 量时有个阻尼因子，如果超量 20G，阻尼因子为 0.1
  // 那么只会 preempt 20 GB * 0.1 = 2GB
  private void calculateResToObtainByPartitionForLeafQueues(
      Set<String> leafQueueNames, Resource clusterResource) {
    // Loop all leaf queues
    for (String queueName : leafQueueNames) {
      // check if preemption disabled for the queue
      if (context.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping from queue=" + queueName
              + " because it's a non-preemptable queue");
        }
        continue;
      }

      // compute resToObtainByPartition considered inter-queue preemption
      for (TempQueuePerPartition qT : context.getQueuePartitions(queueName)) {
        // we act only if we are violating balance by more than
        // maxIgnoredOverCapacity
        // 使用的比配置量大，就会被 preempt
        if (Resources.greaterThan(rc, clusterResource,
            qT.getUsed(), Resources
                .multiply(qT.getGuaranteed(),
                    1.0 + context.getMaxIgnoreOverCapacity()))) {
          /*
           * We introduce a dampening factor naturalTerminationFactor that
           * accounts for natural termination of containers.
           *
           * This is added to control pace of preemption, let's say:
           * If preemption policy calculated a queue *should be* preempted 20 GB
           * And the nature_termination_factor set to 0.1. As a result, preemption
           * policy will select 20 GB * 0.1 = 2GB containers to be preempted.
           *
           * However, it doesn't work for YARN-4390:
           * For example, if a queue needs to be preempted 20GB for *one single*
           * large container, preempt 10% of such resource isn't useful.
           * So to make it simple, only apply nature_termination_factor when
           * selector is not reservedPreemptionCandidatesSelector.
           */
          Resource resToObtain = qT.toBePreempted;
          if (!isReservedPreemptionCandidatesSelector) {
            resToObtain = Resources.multiply(qT.toBePreempted,
                context.getNaturalTerminationFactor());
          }

          // Only add resToObtain when it >= 0
          if (Resources.greaterThan(rc, clusterResource, resToObtain,
              Resources.none())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Queue=" + queueName + " partition=" + qT.partition
                  + " resource-to-obtain=" + resToObtain);
            }
          }
          qT.setActuallyToBePreempted(Resources.clone(resToObtain));
        } else {
          qT.setActuallyToBePreempted(Resources.none());
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(qT);
        }
      }
    }
  }

  private void updatePreemptableExtras(TempQueuePerPartition cur) {
    if (cur.children == null || cur.children.isEmpty()) {
      cur.updatePreemptableExtras(rc);
    } else {
      for (TempQueuePerPartition child : cur.children) {
        updatePreemptableExtras(child);
      }
      cur.updatePreemptableExtras(rc);
    }
  }

  // 计算每个队列实际要被 preempt 的量
  public void computeIdealAllocation(Resource clusterResource,
      Resource totalPreemptionAllowed) {
    for (String partition : context.getAllPartitions()) {
      TempQueuePerPartition tRoot = context.getQueueByPartition(
          CapacitySchedulerConfiguration.ROOT, partition);
      // 这里计算好每个队列超出资源配置的部分，存在 TempQueuePerPartition
      // preemptableExtra 表示可以被抢占的
      // untouchableExtra 表示不可被抢占的（队列配置了不可抢占）
      updatePreemptableExtras(tRoot);

      // compute the ideal distribution of resources among queues
      // updates cloned queues state accordingly
      tRoot.idealAssigned = tRoot.getGuaranteed();
      // 遍历队列树，重新计算资源分配，并计算出每个队列计划要 Preempt 的量
      recursivelyComputeIdealAssignment(tRoot, totalPreemptionAllowed);
    }

    // based on ideal allocation select containers to be preempted from each
    // calculate resource-to-obtain by partition for each leaf queues
    // 计算实际每个队列要被 Preempt 的量 actuallyToBePreempted（有个阻尼因子，不会一下把所有超量的都干掉）
    calculateResToObtainByPartitionForLeafQueues(context.getLeafQueueNames(),
        clusterResource);
  }
}