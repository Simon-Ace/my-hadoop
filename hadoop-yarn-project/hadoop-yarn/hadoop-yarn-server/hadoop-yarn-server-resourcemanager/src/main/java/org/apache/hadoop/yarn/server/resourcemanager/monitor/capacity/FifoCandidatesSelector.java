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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class FifoCandidatesSelector
    extends PreemptionCandidatesSelector {
  private static final Log LOG =
      LogFactory.getLog(FifoCandidatesSelector.class);
  private PreemptableResourceCalculator preemptableAmountCalculator;

  FifoCandidatesSelector(CapacitySchedulerPreemptionContext preemptionContext,
      boolean includeReservedResource) {
    super(preemptionContext);

    preemptableAmountCalculator = new PreemptableResourceCalculator(
        preemptionContext, includeReservedResource);
  }

  // ProportionalCapacityPreemptionPolicy 中默认的抢占选取规则
  @Override
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptionAllowed) {
    // ------------ 第一步 ------------ （根据使用量和需求量重新分配资源）
    // Calculate how much resources we need to preempt
    // 计算出每个资源池每个队列当前资源分配量，和实际要 preempt 的量
    // todo 这里有些奇怪的，还有没满足资源需求的队列在 underServedQueues，没看到后续处理了？
    preemptableAmountCalculator.computeIdealAllocation(clusterResource,
        totalPreemptionAllowed);

    // ------------ 第二步 ------------ （根据资源差额，计算要 kill 的 container）
    // 根据计算得到的要抢占的量，计算各资源池各队列要 kill 的 container
    // Previous selectors (with higher priority) could have already
    // selected containers. We need to deduct preemptable resources
    // based on already selected candidates.
    // 如果有多个规则，会进行这个逻辑
    CapacitySchedulerPreemptionUtils
        .deductPreemptableResourcesBasedSelectedCandidates(preemptionContext,
            selectedCandidates);

    List<RMContainer> skippedAMContainerlist = new ArrayList<>();

    // Loop all leaf queues
    // 这里没有优先级的么？队列不用按顺序来？
    // 可能的原因：前面资源分配都已经做好了，所以在抢占时无所谓先后，都是要被抢的
    // 这里是有优先级的： 使用共享池的资源 -> 队列中后提交的任务 -> amContainer
    for (String queueName : preemptionContext.getLeafQueueNames()) {
      // check if preemption disabled for the queue
      if (preemptionContext.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).preemptionDisabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping from queue=" + queueName
              + " because it's a non-preemptable queue");
        }
        continue;
      }

      // compute resToObtainByPartition considered inter-queue preemption
      LeafQueue leafQueue = preemptionContext.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).leafQueue;

      // 获取该队列在每个资源池要被抢占的量
      Map<String, Resource> resToObtainByPartition =
          CapacitySchedulerPreemptionUtils
              .getResToObtainByPartitionForLeafQueue(preemptionContext,
                  queueName, clusterResource);

      synchronized (leafQueue) {
        // 使用共享池资源的，先处理
        // go through all ignore-partition-exclusivity containers first to make
        // sure such containers will be preemptionCandidates first
        Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityContainers =
            leafQueue.getIgnoreExclusivityRMContainers();
        for (String partition : resToObtainByPartition.keySet()) {
          if (ignorePartitionExclusivityContainers.containsKey(partition)) {
            TreeSet<RMContainer> rmContainers =
                ignorePartitionExclusivityContainers.get(partition);
            // We will check container from reverse order, so latter submitted
            // application's containers will be preemptionCandidates first.
            // 最后提交的任务，会被最先抢占
            for (RMContainer c : rmContainers.descendingSet()) {
              if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
                  selectedCandidates)) {
                // Skip already selected containers
                continue;
              }
              // 将 Container 放到待抢占集合 preemptMap 中
              boolean preempted = CapacitySchedulerPreemptionUtils
                  .tryPreemptContainerAndDeductResToObtain(rc,
                      preemptionContext, resToObtainByPartition, c,
                      clusterResource, selectedCandidates,
                      totalPreemptionAllowed);
              if (!preempted) {
                continue;
              }
            }
          }
        }

        // preempt other containers
        Resource skippedAMSize = Resource.newInstance(0, 0);
        // FiCaSchedulerApp: Represents an application attempt from the viewpoint of the FIFO or Capacity scheduler.
        // 默认是 FifoOrderingPolicy，desc 也就是最后提交的在最前面
        Iterator<FiCaSchedulerApp> desc =
            leafQueue.getOrderingPolicy().getPreemptionIterator();
        while (desc.hasNext()) {
          FiCaSchedulerApp fc = desc.next();
          // When we complete preempt from one partition, we will remove from
          // resToObtainByPartition, so when it becomes empty, we can get no
          // more preemption is needed
          if (resToObtainByPartition.isEmpty()) {
            break;
          }

          // 从 application 中选出要被抢占的容器
          preemptFrom(fc, clusterResource, resToObtainByPartition,
              skippedAMContainerlist, skippedAMSize, selectedCandidates,
              totalPreemptionAllowed);
        }

        // Can try preempting AMContainers (still saving atmost
        // maxAMCapacityForThisQueue AMResource's) if more resources are
        // required to be preemptionCandidates from this Queue.
        Resource maxAMCapacityForThisQueue = Resources.multiply(
            Resources.multiply(clusterResource,
                leafQueue.getAbsoluteCapacity()),
            leafQueue.getMaxAMResourcePerQueuePercent());

        preemptAMContainers(clusterResource, selectedCandidates, skippedAMContainerlist,
            resToObtainByPartition, skippedAMSize, maxAMCapacityForThisQueue,
            totalPreemptionAllowed);
      }
    }

    return selectedCandidates;
  }

  /**
   * As more resources are needed for preemption, saved AMContainers has to be
   * rescanned. Such AMContainers can be preemptionCandidates based on resToObtain, but
   * maxAMCapacityForThisQueue resources will be still retained.
   *
   * @param clusterResource
   * @param preemptMap
   * @param skippedAMContainerlist
   * @param skippedAMSize
   * @param maxAMCapacityForThisQueue
   */
  private void preemptAMContainers(Resource clusterResource,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      List<RMContainer> skippedAMContainerlist,
      Map<String, Resource> resToObtainByPartition, Resource skippedAMSize,
      Resource maxAMCapacityForThisQueue, Resource totalPreemptionAllowed) {
    for (RMContainer c : skippedAMContainerlist) {
      // Got required amount of resources for preemption, can stop now
      if (resToObtainByPartition.isEmpty()) {
        break;
      }
      // Once skippedAMSize reaches down to maxAMCapacityForThisQueue,
      // container selection iteration for preemption will be stopped.
      if (Resources.lessThanOrEqual(rc, clusterResource, skippedAMSize,
          maxAMCapacityForThisQueue)) {
        break;
      }

      boolean preempted = CapacitySchedulerPreemptionUtils
          .tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
              resToObtainByPartition, c, clusterResource, preemptMap,
              totalPreemptionAllowed);
      if (preempted) {
        Resources.subtractFrom(skippedAMSize, c.getAllocatedResource());
      }
    }
    skippedAMContainerlist.clear();
  }

  /**
   * Given a target preemption for a specific application, select containers
   * to preempt (after unreserving all reservation for that app).
   */
  @SuppressWarnings("unchecked")
  // 从 application 中选出要被抢占的容器
  private void preemptFrom(FiCaSchedulerApp app,
      Resource clusterResource, Map<String, Resource> resToObtainByPartition,
      List<RMContainer> skippedAMContainerlist, Resource skippedAMSize,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedContainers,
      Resource totalPreemptionAllowed) {
    ApplicationAttemptId appId = app.getApplicationAttemptId();

    // first drop reserved containers towards rsrcPreempt
    // reservedContainers 大概跟了下，可能是有资源需求增长的 Container
    List<RMContainer> reservedContainers =
        new ArrayList<>(app.getReservedContainers());
    for (RMContainer c : reservedContainers) {
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
          selectedContainers)) {
        continue;
      }
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      // Try to preempt this container
      CapacitySchedulerPreemptionUtils.tryPreemptContainerAndDeductResToObtain(
          rc, preemptionContext, resToObtainByPartition, c, clusterResource,
          selectedContainers, totalPreemptionAllowed);

      if (!preemptionContext.isObserveOnly()) {
        preemptionContext.getRMContext().getDispatcher().getEventHandler()
            .handle(new ContainerPreemptEvent(appId, c,
                SchedulerEventType.KILL_RESERVED_CONTAINER));
      }
    }

    // if more resources are to be freed go through all live containers in
    // reverse priority and reverse allocation order and mark them for
    // preemption
    List<RMContainer> liveContainers =
        new ArrayList<>(app.getLiveContainers());
    // 按照 ContainerId、attemptId 排序（推测应该也是后入先出）
    sortContainers(liveContainers);

    for (RMContainer c : liveContainers) {
      if (resToObtainByPartition.isEmpty()) {
        return;
      }

      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
          selectedContainers)) {
        continue;
      }

      // Skip already marked to killable containers
      if (null != preemptionContext.getKillableContainers() && preemptionContext
          .getKillableContainers().contains(c.getContainerId())) {
        continue;
      }

      // Skip AM Container from preemption for now.
      if (c.isAMContainer()) {
        skippedAMContainerlist.add(c);
        Resources.addTo(skippedAMSize, c.getAllocatedResource());
        continue;
      }

      // Try to preempt this container
      CapacitySchedulerPreemptionUtils.tryPreemptContainerAndDeductResToObtain(
          rc, preemptionContext, resToObtainByPartition, c, clusterResource,
          selectedContainers, totalPreemptionAllowed);
    }
  }
}
