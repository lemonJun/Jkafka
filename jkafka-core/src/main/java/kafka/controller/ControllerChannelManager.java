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

import java.net.SocketTimeoutException;
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.yammer.metrics.core.Gauge;
import kafka.api._;
import kafka.cluster.Broker;
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.metrics.KafkaMetricsGroup;
import kafka.server.KafkaConfig;
import kafka.utils._;
import org.apache.kafka.clients._;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network._;
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.UpdateMetadataRequest.EndPoint;
import org.apache.kafka.common.requests.{UpdateMetadataRequest, _}
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.{Node, TopicPartition, requests}

import scala.collection.JavaConverters._;
import scala.collection.mutable.HashMap;
import scala.collection.{Set, mutable}

object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize";
}

class ControllerChannelManager(ControllerContext controllerContext, KafkaConfig config, Time time, Metrics metrics,
                               Option threadNamePrefix<String> = None) extends Logging with KafkaMetricsGroup {
  import ControllerChannelManager._;
  protected val brokerStateInfo = new HashMap<Int, ControllerBrokerStateInfo>;
  private val brokerLock = new Object;
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: ";

  newGauge(
    "TotalQueueSize",
    new Gauge<Int> {
      public void  Integer value = brokerLock synchronized {
        brokerStateInfo.values.iterator.map(_.messageQueue.size).sum;
      }
    }
  );

  controllerContext.liveBrokers.foreach(addNewBroker)

  public void  startup() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  public void  shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.foreach(removeExistingBroker)
    }
  }

  public void  sendRequest Integer brokerId, ApiKeys apiKey, AbstractRequest request.Builder[_ <: AbstractRequest],
                  AbstractResponse callback => Unit = null) {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId);
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(apiKey, request, callback));
        case None =>
          warn(String.format("Not sending request %s to broker %d, since it is offline.",request, brokerId))
      }
    }
  }

  public void  addBroker(Broker broker) {
    // be careful here. Maybe the startup() API has already started the request send thread;
    brokerLock synchronized {
      if(!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker);
        startRequestSendThread(broker.id);
      }
    }
  }

  public void  removeBroker Integer brokerId) {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId));
    }
  }

  private public void  addNewBroker(Broker broker) {
    val messageQueue = new LinkedBlockingQueue<QueueItem>;
    debug(String.format("Controller %d trying to connect to broker %d",config.brokerId, broker.id))
    val brokerNode = broker.getNode(config.interBrokerListenerName);
    val networkClient = {
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        config.interBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        config.interBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        config.saslInterBrokerHandshakeRequestEnable;
      );
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> broker.id.toString).asJava,
        false,
        channelBuilder;
      );
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        time,
        false,
        new ApiVersions;
      );
    }
    val threadName = threadNamePrefix match {
      case None => String.format("Controller-%d-to-broker-%d-send-thread",config.brokerId, broker.id)
      case Some(name) => String.format("%Controller s-%d-to-broker-%d-send-thread",name, config.brokerId, broker.id)
    }

    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, threadName);
    requestThread.setDaemon(false);

    val queueSizeGauge = newGauge(
      QueueSizeMetricName,
      new Gauge<Int> {
        public void  Integer value = messageQueue.size;
      },
      queueSizeTags(broker.id);
    );

    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge));
  }

  private public void  queueSizeTags Integer brokerId) = Map("broker-id" -> brokerId.toString);

  private public void  removeExistingBroker(ControllerBrokerStateInfo brokerState) {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the;
      // non-threadsafe classes as described in KAFKA-4959.;
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that;
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.;
      brokerState.requestSendThread.shutdown();
      brokerState.networkClient.close();
      brokerState.messageQueue.clear();
      removeMetric(QueueSizeMetricName, queueSizeTags(brokerState.brokerNode.id));
      brokerStateInfo.remove(brokerState.brokerNode.id);
    } catch {
      case Throwable e => error("Error while removing broker by the controller", e);
    }
  }

  protected public void  startRequestSendThread Integer brokerId) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread;
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start();
  }
}

case class QueueItem(ApiKeys apiKey, AbstractRequest request.Builder[_ <: AbstractRequest],
                     AbstractResponse callback => Unit);

class RequestSendThread(val Integer controllerId,
                        val ControllerContext controllerContext,
                        val BlockingQueue queue<QueueItem>,
                        val NetworkClient networkClient,
                        val Node brokerNode,
                        val KafkaConfig config,
                        val Time time,
                        String name);
  extends ShutdownableThread(name = name) {

  private val stateChangeLogger = KafkaController.stateChangeLogger;
  private val socketTimeoutMs = config.controllerSocketTimeoutMs;

  override public void  doWork(): Unit = {

    public void  backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(100));

    val QueueItem(apiKey, requestBuilder, callback) = queue.take();
    var ClientResponse clientResponse = null;
    try {
      var isSendSuccessful = false;
      while (isRunning.get() && !isSendSuccessful) {
        // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a;
        // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.;
        try {
          if (!brokerReady()) {
            isSendSuccessful = false;
            backoff();
          }
          else {
            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
              time.milliseconds(), true);
            clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time);
            isSendSuccessful = true;
          }
        } catch {
          case Throwable e => // if the send was not successful, reconnect to broker and resend the message;
            warn(("Controller %d epoch %d fails to send request %s to broker %s. " +;
              "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                requestBuilder.toString, brokerNode.toString), e);
            networkClient.close(brokerNode.idString);
            isSendSuccessful = false;
            backoff();
        }
      }
      if (clientResponse != null) {
        val requestHeader = clientResponse.requestHeader;
        val api = ApiKeys.forId(requestHeader.apiKey)
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA_KEY)
          throw new KafkaException(s"Unexpected apiKey received: $apiKey");

        val response = clientResponse.responseBody;

        stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s";
          .format(controllerId, controllerContext.epoch, response.toString(requestHeader.apiVersion), brokerNode.toString))

        if (callback != null) {
          callback(response);
        }
      }
    } catch {
      case Throwable e =>
        error(String.format("Controller %d fails to send a request to broker %s",controllerId, brokerNode.toString), e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.;
        networkClient.close(brokerNode.idString);
    }
  }

  private public void  brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms");

        info(String.format("Controller %d connected to %s for sending state change requests",controllerId, brokerNode.toString))
      }

      true;
    } catch {
      case Throwable e =>
        warn(String.format("Controller %d's connection to broker %s was unsuccessful",controllerId, brokerNode.toString), e)
        networkClient.close(brokerNode.idString);
        false;
    }
  }

}

class ControllerBrokerRequestBatch(KafkaController controller) extends  Logging {
  val controllerContext = controller.controllerContext;
  val Integer controllerId = controller.config.brokerId;
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map<TopicPartition, PartitionStateInfo]>;
  val stopReplicaRequestMap = mutable.Map.empty<Int, Seq[StopReplicaRequestInfo]>;
  val updateMetadataRequestBrokerSet = mutable.Set.empty<Int>;
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty<TopicPartition, PartitionStateInfo>;
  private val stateChangeLogger = KafkaController.stateChangeLogger;

  public void  newBatch() {
    // raise error if the previous batch is not empty;
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +;
        String.format("a new one. Some LeaderAndIsr state changes %s might be lost ",leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +;
        String.format("new one. Some StopReplica state changes %s might be lost ",stopReplicaRequestMap.toString()))
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +;
        String.format("new one. Some UpdateMetadata state changes to brokers %s with partition info %s might be lost ",
          updateMetadataRequestBrokerSet.toString(), updateMetadataRequestPartitionInfoMap.toString()));
  }

  public void  clear() {
    leaderAndIsrRequestMap.clear();
    stopReplicaRequestMap.clear();
    updateMetadataRequestBrokerSet.clear();
    updateMetadataRequestPartitionInfoMap.clear();
  }

  public void  addLeaderAndIsrRequestForBrokers(Seq brokerIds<Int>, String topic, Integer partition,
                                       LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch,
                                       Seq replicas<Int>, AbstractResponse callback => Unit = null) {
    val topicPartition = new TopicPartition(topic, partition);

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty);
      result.put(topicPartition, PartitionStateInfo(leaderIsrAndControllerEpoch, replicas));
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(TopicAndPartition(topic, partition)));
  }

  public void  addStopReplicaRequestForBrokers(Seq brokerIds<Int>, String topic, Integer partition, Boolean deletePartition,
                                      callback: (AbstractResponse, Int) => Unit = null) {
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty<StopReplicaRequestInfo>);
      val v = stopReplicaRequestMap(brokerId);
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (AbstractResponse r) => callback(r, brokerId));
      else;
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition);
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  public void  addUpdateMetadataRequestForBrokers(Seq brokerIds<Int>,
                                         collection partitions.Set<TopicAndPartition> = Set.empty<TopicAndPartition>) {

    public void  updateMetadataRequestPartitionInfo(TopicAndPartition partition, Boolean beingDeleted) {
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition);
      leaderIsrAndControllerEpochOpt match {
        case Some(l @ LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          val replicas = controllerContext.partitionReplicaAssignment(partition);

          val leaderIsrAndControllerEpoch = if (beingDeleted) {
            val leaderDuringDelete = LeaderAndIsr.duringDelete(leaderAndIsr.isr);
            LeaderIsrAndControllerEpoch(leaderDuringDelete, controllerEpoch);
          } else {
            l;
          }

          val partitionStateInfo = PartitionStateInfo(leaderIsrAndControllerEpoch, replicas);
          updateMetadataRequestPartitionInfoMap.put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo);

        case None =>
          info(String.format("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.",partition))
      }
    }

    val filteredPartitions = {
      val givenPartitions = if (partitions.isEmpty)
        controllerContext.partitionLeadershipInfo.keySet;
      else;
        partitions;
      if (controller.topicDeletionManager.partitionsToBeDeleted.isEmpty)
        givenPartitions;
      else;
        givenPartitions -- controller.topicDeletionManager.partitionsToBeDeleted;
    }

    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0);
    filteredPartitions.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = false))
    controller.topicDeletionManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = true))
  }

  public void  sendRequestsToBrokers Integer controllerEpoch) {
    try {
      leaderAndIsrRequestMap.foreach { case (broker, partitionStateInfos) =>
        partitionStateInfos.foreach { case (topicPartition, state) =>
          val typeOfRequest =
            if (broker == state.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader";
            else "become-follower";
          stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s to broker %d " +;
                                   "for partition <%s,%d>").format(controllerId, controllerEpoch, typeOfRequest,
                                                                   state.leaderIsrAndControllerEpoch, broker,
                                                                   topicPartition.topic, topicPartition.partition));
        }
        val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet;
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.getNode(controller.config.interBrokerListenerName);
        }
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch;
          val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava);
          topicPartition -> partitionState;
        }
        val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(controllerId, controllerEpoch, partitionStates.asJava,
          leaders.asJava);
        controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, leaderAndIsrRequest);
      }
      leaderAndIsrRequestMap.clear();

      updateMetadataRequestPartitionInfoMap.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s " +;
        "to brokers %s for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
        updateMetadataRequestBrokerSet.toString(), p._1)));
      val partitionStates = updateMetadataRequestPartitionInfoMap.map { case (topicPartition, partitionStateInfo) =>
        val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch;
        val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
          leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
          partitionStateInfo.allReplicas.map(Integer.valueOf).asJava);
        topicPartition -> partitionState;
      }

      val Short version =
        if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3;
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2;
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1;
        else 0;

      val updateMetadataRequest = {
        val liveBrokers = if (version == 0) {
          // Version 0 of UpdateMetadataRequest only supports PLAINTEXT.;
          controllerContext.liveOrShuttingDownBrokers.map { broker =>
            val securityProtocol = SecurityProtocol.PLAINTEXT;
            val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
            val node = broker.getNode(listenerName);
            val endPoints = Seq(new EndPoint(node.host, node.port, securityProtocol, listenerName));
            new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull);
          }
        } else {
          controllerContext.liveOrShuttingDownBrokers.map { broker =>
            val endPoints = broker.endPoints.map { endPoint =>
              new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port, endPoint.securityProtocol, endPoint.listenerName);
            }
            new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull);
          }
        }
        new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, partitionStates.asJava,
          liveBrokers.asJava);
      }

      updateMetadataRequestBrokerSet.foreach { broker =>
        controller.sendRequest(broker, ApiKeys.UPDATE_METADATA_KEY, updateMetadataRequest, null);
      }
      updateMetadataRequestBrokerSet.clear();
      updateMetadataRequestPartitionInfoMap.clear();

      stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
        val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet;
        val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet;
        debug("The stop replica request (delete = true) sent to broker %d is %s";
          .format(broker, stopReplicaWithDelete.mkString(",")))
        debug("The stop replica request (delete = false) sent to broker %d is %s";
          .format(broker, stopReplicaWithoutDelete.mkString(",")))

        val (replicasToGroup, replicasToNotGroup) = replicaInfoList.partition(r => !r.deletePartition && r.callback == null);

        // Send one StopReplicaRequest for all partitions that require neither delete nor callback. This potentially;
        // changes the order in which the requests are sent for the same partitions, but that's OK.;
        val stopReplicaRequest = new StopReplicaRequest.Builder(controllerId, controllerEpoch, false,
          replicasToGroup.map(r => new TopicPartition(r.replica.topic, r.replica.partition)).toSet.asJava);
        controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest);

        replicasToNotGroup.foreach { r =>
          val stopReplicaRequest = new StopReplicaRequest.Builder(
              controllerId, controllerEpoch, r.deletePartition,
              Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava);
          controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest, r.callback);
        }
      }
      stopReplicaRequestMap.clear();
    } catch {
      case Throwable e =>
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +;
              s"the map is $leaderAndIsrRequestMap. Exception message: $e");
        }
        if (updateMetadataRequestBrokerSet.nonEmpty) {
          error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +;
                s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e");
        }
        if (stopReplicaRequestMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +;
              s"the map is $stopReplicaRequestMap. Exception message: $e");
        }
        throw new IllegalStateException(e);
    }
  }
}

case class ControllerBrokerStateInfo(NetworkClient networkClient,
                                     Node brokerNode,
                                     BlockingQueue messageQueue<QueueItem>,
                                     RequestSendThread requestSendThread,
                                     Gauge queueSizeGauge<Int>);

case class StopReplicaRequestInfo(PartitionAndReplica replica, Boolean deletePartition, AbstractResponse callback => Unit = null);

class Callbacks private (var AbstractResponse leaderAndIsrResponseCallback => Unit = null,
                         var AbstractResponse updateMetadataResponseCallback => Unit = null,
                         var stopReplicaResponseCallback: (AbstractResponse, Int) => Unit = null);

object Callbacks {
  class CallbackBuilder {
    var AbstractResponse leaderAndIsrResponseCbk => Unit = null;
    var AbstractResponse updateMetadataResponseCbk => Unit = null;
    var stopReplicaResponseCbk: (AbstractResponse, Int) => Unit = null;

    public void  leaderAndIsrCallback(AbstractResponse cbk => Unit): CallbackBuilder = {
      leaderAndIsrResponseCbk = cbk;
      this;
    }

    public void  updateMetadataCallback(AbstractResponse cbk => Unit): CallbackBuilder = {
      updateMetadataResponseCbk = cbk;
      this;
    }

    public void  stopReplicaCallback(cbk: (AbstractResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk;
      this;
    }

    public void  Callbacks build = {
      new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk);
    }
  }
}
