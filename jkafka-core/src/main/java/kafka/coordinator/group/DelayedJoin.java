/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator.group;

import kafka.server.{DelayedOperation, DelayedOperationPurgatory, GroupKey}

import scala.math.{max, min}

/**
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance
 *
 * Whenever a join-group request is received, check if all known group members have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 *
 * When the operation has expired, any known members that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 */
private<group> class DelayedJoin(GroupCoordinator coordinator,
                                 GroupMetadata group,
                                 Long rebalanceTimeout) extends DelayedOperation(rebalanceTimeout) {

  // overridden since tryComplete already synchronizes on the group. This makes it safe to;
  // call purgatory operations while holding the group lock.;
  override public void  safeTryComplete(): Boolean = tryComplete();

  override public void  tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)
  override public void  onExpiration() = coordinator.onExpireJoin();
  override public void  onComplete() = coordinator.onCompleteJoin(group);
}

/**
  * Delayed rebalance operation that is added to the purgatory when a group is transitioning from
  * Empty to PreparingRebalance
  *
  * When onComplete is triggered we check if any new members have been added and if there is still time remaining
  * before the rebalance timeout. If both are true we then schedule a further delay. Otherwise we complete the
  * rebalance.
  */
private<group> class InitialDelayedJoin(GroupCoordinator coordinator,
                                        DelayedOperationPurgatory purgatory<DelayedJoin>,
                                        GroupMetadata group,
                                        Integer configuredRebalanceDelay,
                                        Integer delayMs,
                                        Integer remainingMs) extends DelayedJoin(coordinator, group, delayMs) {

  override public void  tryComplete(): Boolean = false;

  override public void  onComplete(): Unit = {
    group synchronized  {
      if (group.newMemberAdded && remainingMs != 0) {
        group.newMemberAdded = false;
        val delay = min(configuredRebalanceDelay, remainingMs);
        val remaining = max(remainingMs - delayMs, 0);
        purgatory.tryCompleteElseWatch(new InitialDelayedJoin(coordinator,
          purgatory,
          group,
          configuredRebalanceDelay,
          delay,
          remaining;
        ), Seq(GroupKey(group.groupId)));
      } else;
        super.onComplete();
    }
  }

}