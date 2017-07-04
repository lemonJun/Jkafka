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
package kafka.controller;

import kafka.api.LeaderAndIsr;
import kafka.common.{LeaderElectionNotNeededException, NoReplicaOnlineException, StateChangeFailedException, TopicAndPartition}
import kafka.controller.Callbacks.CallbackBuilder;
import kafka.utils.ZkUtils._;
import kafka.utils.{Logging, ReplicationUtils}
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import scala.collection._;

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. This NonExistentPartition state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
class PartitionStateMachine(KafkaController controller) extends Logging {
  private val controllerContext = controller.controllerContext;
  private val controllerId = controller.config.brokerId;
  private val zkUtils = controllerContext.zkUtils;
  private val mutable partitionState.Map<TopicAndPartition, PartitionState> = mutable.Map.empty;
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller);
  private val noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext);

  private val stateChangeLogger = KafkaController.stateChangeLogger;

  this.logIdent = "[Partition state machine on Controller " + controllerId + "]: ";

  /**
   * Invoked on successful controller election. First registers a topic change listener since that triggers all
   * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
   * the OnlinePartition state change for all new or offline partitions.
   */
  public void  startup() {
    initializePartitionState();
    triggerOnlinePartitionStateChange();

    info("Started partition state machine with initial state -> " + partitionState.toString());
  }

  /**
   * Invoked on controller shutdown.
   */
  public void  shutdown() {
    partitionState.clear();

    info("Stopped partition state machine");
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  public void  triggerOnlinePartitionStateChange() {
    try {
      brokerRequestBatch.newBatch();
      // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions;
      // that belong to topics to be deleted;
      for ((topicAndPartition, partitionState) <- partitionState;
          if !controller.topicDeletionManager.isTopicQueuedUpForDeletion(topicAndPartition.topic)) {
        if (partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
          handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                            (new CallbackBuilder).build);
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch);
    } catch {
      case Throwable e => error("Error while moving some partitions to the online state", e);
      // It TODO is not enough to bail out and log an error, it is important to trigger leader election for those partitions;
    }
  }

  public void  partitionsInState(PartitionState state): Set<TopicAndPartition> = {
    partitionState.filter(p => p._2 == state).keySet;
  }

  /**
   * This API is invoked by the partition change zookeeper listener
   * @param partitions   The list of partitions that need to be transitioned to the target state
   * @param targetState  The state that the partitions should be moved to
   */
  public void  handleStateChanges(Set partitions<TopicAndPartition>, PartitionState targetState,
                         PartitionLeaderSelector leaderSelector = noOpPartitionLeaderSelector,
                         Callbacks callbacks = (new CallbackBuilder).build) {
    info(String.format("Invoking state change to %s for partitions %s",targetState, partitions.mkString(",")))
    try {
      brokerRequestBatch.newBatch();
      partitions.foreach { topicAndPartition =>
        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks);
      }
      brokerRequestBatch.sendRequestsToBrokers(controller.epoch);
    } catch {
      case Throwable e => error(String.format("Error while moving some partitions to %s state",targetState), e)
      // It TODO is not enough to bail out and log an error, it is important to trigger state changes for those partitions;
    }
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param topic       The topic of the partition for which the state transition is invoked
   * @param partition   The partition for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   */
  private public void  handleStateChange(String topic, Integer partition, PartitionState targetState,
                                PartitionLeaderSelector leaderSelector,
                                Callbacks callbacks) {
    val topicAndPartition = TopicAndPartition(topic, partition);
    val currState = partitionState.getOrElseUpdate(topicAndPartition, NonExistentPartition);
    try {
      assertValidTransition(topicAndPartition, targetState);
      targetState match {
        case NewPartition =>
          partitionState.put(topicAndPartition, NewPartition);
          val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).mkString(",");
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s";
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState,
                                            assignedReplicas));
          // partition post has been assigned replicas;
        case OnlinePartition =>
          partitionState(topicAndPartition) match {
            case NewPartition =>
              // initialize leader and isr path for new partition;
              initializeLeaderAndIsrForPartition(topicAndPartition);
            case OfflinePartition =>
              electLeaderForPartition(topic, partition, leaderSelector);
            case OnlinePartition => // invoked when the leader needs to be re-elected;
              electLeaderForPartition(topic, partition, leaderSelector);
            case _ => // should never come here since illegal previous states are checked above;
          }
          partitionState.put(topicAndPartition, OnlinePartition);
          val leader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader;
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s from %s to %s with leader %d";
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState, leader))
           // partition post has a leader;
        case OfflinePartition =>
          // should be called when the leader for a partition is no longer alive;
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s";
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          partitionState.put(topicAndPartition, OfflinePartition);
          // partition post has no alive leader;
        case NonExistentPartition =>
          stateChangeLogger.trace("Controller %d epoch %d changed partition %s state from %s to %s";
                                    .format(controllerId, controller.epoch, topicAndPartition, currState, targetState))
          partitionState.put(topicAndPartition, NonExistentPartition);
          // partition post state is deleted from all brokers and zookeeper;
      }
    } catch {
      case Throwable t =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change for partition %s from %s to %s failed";
          .format(controllerId, controller.epoch, topicAndPartition, currState, targetState), t)
    }
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private public void  initializePartitionState() {
    for (topicPartition <- controllerContext.partitionReplicaAssignment.keys) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state;
      controllerContext.partitionLeadershipInfo.get(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state;
          if (controllerContext.liveBrokerIds.contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader))
            // leader is alive;
            partitionState.put(topicPartition, OnlinePartition);
          else;
            partitionState.put(topicPartition, OfflinePartition);
        case None =>
          partitionState.put(topicPartition, NewPartition);
      }
    }
  }

  private public void  assertValidTransition(TopicAndPartition topicAndPartition, PartitionState targetState): Unit = {
    if (!targetState.validPreviousStates.contains(partitionState(topicAndPartition)))
      throw new IllegalStateException("Partition %s should be in the %s states before moving to %s state";
        .format(topicAndPartition, targetState.validPreviousStates.mkString(","), targetState) + ". Instead it is in %s state";
        .format(partitionState(topicAndPartition)))
  }

  /**
   * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
   * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, its leader and isr
   * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
   * OfflinePartition state.
   * @param topicAndPartition   The topic/partition whose leader and isr path is to be initialized
   */
  private public void  initializeLeaderAndIsrForPartition(TopicAndPartition topicAndPartition) = {
    val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition).toList;
    val liveAssignedReplicas = replicaAssignment.filter(controllerContext.liveBrokerIds.contains);
    liveAssignedReplicas.headOption match {
      case None =>
        val failMsg = s"Controller $controllerId epoch ${controller.epoch} encountered error during state change of " +;
          s"partition $topicAndPartition from New to Online, assigned replicas are " +;
          s"<${replicaAssignment.mkString(",")}>, live brokers are <${controllerContext.liveBrokerIds}>. No assigned " +;
          "replica is alive.";

        stateChangeLogger.error(failMsg);
        throw new StateChangeFailedException(failMsg);

      // leader is the first replica in the list of assigned replicas;
      case Some(leader) =>
        debug(s"Live assigned replicas for partition $topicAndPartition are: <$liveAssignedReplicas>")
        val leaderAndIsr = LeaderAndIsr(leader, liveAssignedReplicas);
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controller.epoch);
        debug(s"Initializing leader and isr for partition $topicAndPartition to $leaderIsrAndControllerEpoch")

        try {
          zkUtils.createPersistentPath(
            getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
            zkUtils.leaderAndIsrZkData(leaderAndIsr, controller.epoch);
          );
          // the NOTE above write can fail only if the current controller lost its zk session and the new controller;
          // took over and initialized this partition. This can happen if the current controller went into a long;
          // GC pause;
          controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch);
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(
            liveAssignedReplicas,
            topicAndPartition.topic,
            topicAndPartition.partition,
            leaderIsrAndControllerEpoch,
            replicaAssignment;
          );
        } catch {
          case ZkNodeExistsException _ =>
            // read the controller epoch;
            val leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topicAndPartition.topic,
              topicAndPartition.partition).get;

            val failMsg = s"Controller $controllerId epoch ${controller.epoch} encountered error while changing " +;
              s"partition $topicAndPartition's state from New to Online since LeaderAndIsr path already exists with " +;
              s"value ${leaderIsrAndEpoch.leaderAndIsr} and controller epoch ${leaderIsrAndEpoch.controllerEpoch}";
            stateChangeLogger.error(failMsg);
            throw new StateChangeFailedException(failMsg);
        }
    }
  }

  /**
   * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change.
   * It invokes the leader election API to elect a leader for the input offline partition
   * @param topic               The topic of the offline partition
   * @param partition           The offline partition
   * @param leaderSelector      Specific leader selector (e.g., offline/reassigned/etc.)
   */
  public void  electLeaderForPartition(String topic, Integer partition, PartitionLeaderSelector leaderSelector) {
    val topicAndPartition = TopicAndPartition(topic, partition);
    // handle leader election for the partitions whose leader is no longer alive;
    stateChangeLogger.trace("Controller %d epoch %d started leader election for partition %s";
                              .format(controllerId, controller.epoch, topicAndPartition))
    try {
      var Boolean zookeeperPathUpdateSucceeded = false;
      var LeaderAndIsr newLeaderAndIsr = null;
      var Seq replicasForThisPartition<Int> = Seq.empty<Int>;
      while(!zookeeperPathUpdateSucceeded) {
        val currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition);
        val currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr;
        val controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch;
        if (controllerEpoch > controller.epoch) {
          val failMsg = ("aborted leader election for partition <%s,%d> since the LeaderAndIsr path was " +;
                         "already written by another controller. This probably means that the current controller %d went through " +;
                         "a soft failure and another controller was elected with epoch %d.");
                           .format(topic, partition, controllerId, controllerEpoch)
          stateChangeLogger.error(String.format("Controller %d epoch %d ",controllerId, controller.epoch) + failMsg)
          throw new StateChangeFailedException(failMsg);
        }
        // elect new leader or throw exception;
        val (leaderAndIsr, replicas) = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr);
        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partition,
          leaderAndIsr, controller.epoch, currentLeaderAndIsr.zkVersion);
        newLeaderAndIsr = leaderAndIsr.withZkVersion(newVersion);
        zookeeperPathUpdateSucceeded = updateSucceeded;
        replicasForThisPartition = replicas;
      }
      val newLeaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch);
      // update the leader cache;
      controllerContext.partitionLeadershipInfo.put(TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch);
      stateChangeLogger.trace("Controller %d epoch %d elected leader %d for Offline partition %s";
                                .format(controllerId, controller.epoch, newLeaderAndIsr.leader, topicAndPartition))
      val replicas = controllerContext.partitionReplicaAssignment(TopicAndPartition(topic, partition));
      // store new leader and isr info in cache;
      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
        newLeaderIsrAndControllerEpoch, replicas);
    } catch {
      case LeaderElectionNotNeededException _ => // swallow;
      case NoReplicaOnlineException nroe => throw nroe;
      case Throwable sce =>
        val failMsg = String.format("encountered error while electing leader for partition %s due to: %s.",topicAndPartition, sce.getMessage)
        stateChangeLogger.error(String.format("Controller %d epoch %d ",controllerId, controller.epoch) + failMsg)
        throw new StateChangeFailedException(failMsg, sce);
    }
    debug(String.format("After leader election, leader cache is updated to %s",controllerContext.partitionLeadershipInfo.map(l => (l._1, l._2))))
  }

  private public void  getLeaderIsrAndEpochOrThrowException(String topic, Integer partition): LeaderIsrAndControllerEpoch = {
    val topicAndPartition = TopicAndPartition(topic, partition);
    ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition) match {
      case Some(currentLeaderIsrAndEpoch) => currentLeaderIsrAndEpoch;
      case None =>
        val failMsg = "LeaderAndIsr information doesn't exist for partition %s in %s state";
                        .format(topicAndPartition, partitionState(topicAndPartition))
        throw new StateChangeFailedException(failMsg);
    }
  }
}

sealed trait PartitionState {
  public void  Byte state;
  public void  Set validPreviousStates<PartitionState>;
}

case object NewPartition extends PartitionState {
  val Byte state = 0;
  val Set validPreviousStates<PartitionState> = Set(NonExistentPartition);
}

case object OnlinePartition extends PartitionState {
  val Byte state = 1;
  val Set validPreviousStates<PartitionState> = Set(NewPartition, OnlinePartition, OfflinePartition);
}

case object OfflinePartition extends PartitionState {
  val Byte state = 2;
  val Set validPreviousStates<PartitionState> = Set(NewPartition, OnlinePartition, OfflinePartition);
}

case object NonExistentPartition extends PartitionState {
  val Byte state = 3;
  val Set validPreviousStates<PartitionState> = Set(OfflinePartition);
}
