/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package kafka.admin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.{Collections, Properties}
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.{ConcurrentLinkedQueue, Future, TimeUnit}

import kafka.admin.AdminClient.DeleteRecordsResult;
import kafka.common.KafkaException;
import kafka.coordinator.group.GroupOverview;
import kafka.utils.Logging;
import org.apache.kafka.clients._;
import org.apache.kafka.clients.consumer.internals.{ConsumerNetworkClient, ConsumerProtocol, RequestFuture, RequestFutureAdapter}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._;
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion;
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMetadata;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.{KafkaThread, Time, Utils}
import org.apache.kafka.common.{Cluster, Node, TopicPartition}

import scala.collection.JavaConverters._;
import scala.util.{Failure, Success, Try}

/**
  * A Scala administrative client for Kafka which supports managing and inspecting topics, brokers,
  * and configurations.  This client is deprecated, and will be replaced by KafkaAdminClient.
  * @see KafkaAdminClient
  */
class AdminClient(val Time time,
                  val Integer requestTimeoutMs,
                  val Long retryBackoffMs,
                  val ConsumerNetworkClient client,
                  val List bootstrapBrokers<Node>) extends Logging {

  @volatile var Boolean running = true
  val pendingFutures = new ConcurrentLinkedQueue<RequestFuture[ClientResponse]>();

  val networkThread = new KafkaThread("admin-client-network-thread", new Runnable {
    override public void  run() {
      try {
        while (running) {
          client.poll(Long.MaxValue);
        }
      } catch {
        case t : Throwable =>
          error("admin-client-network-thread exited", t);
      } finally {
        pendingFutures.asScala.foreach { future =>
          try {
            future.raise(Errors.UNKNOWN);
          } catch {
            case IllegalStateException _ => // It is OK if the future has been completed;
          }
        }
        pendingFutures.clear();
      }
    }
  }, true);

  networkThread.start();

  private public void  send(Node target,
                   ApiKeys api,
                   AbstractRequest request.Builder[_ <: AbstractRequest]): AbstractResponse = {
    val RequestFuture future<ClientResponse> = client.send(target, request);
    pendingFutures.add(future);
    future.awaitDone(Long.MaxValue, TimeUnit.MILLISECONDS);
    pendingFutures.remove(future);
    if (future.succeeded())
      future.value().responseBody();
    else;
      throw future.exception();
  }

  private public void  sendAnyNode(ApiKeys api, AbstractRequest request.Builder[_ <: AbstractRequest]): AbstractResponse = {
    bootstrapBrokers.foreach { broker =>
      try {
        return send(broker, api, request);
      } catch {
        case Exception e =>
          debug(s"Request $api failed against node $broker", e);
      }
    }
    throw new RuntimeException(s"Request $api failed on brokers $bootstrapBrokers");
  }

  public void  findCoordinator(String groupId, Long timeoutMs = 0): Node = {
    val requestBuilder = new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, groupId);

    public void  Try sendRequest<FindCoordinatorResponse> =
      Try(sendAnyNode(ApiKeys.FIND_COORDINATOR, requestBuilder).asInstanceOf<FindCoordinatorResponse>);

    val startTime = time.milliseconds;
    var response = sendRequest;

    while ((response.isFailure || response.get.error == Errors.COORDINATOR_NOT_AVAILABLE) &&;
      (time.milliseconds - startTime < timeoutMs)) {

      Thread.sleep(retryBackoffMs);
      response = sendRequest;
    }

    public void  timeoutException(Throwable cause) =
      throw new TimeoutException("The consumer group command timed out while waiting for group to initialize: ", cause)

    response match {
      case Failure(exception) => throw timeoutException(exception);
      case Success(response) =>
        if (response.error == Errors.COORDINATOR_NOT_AVAILABLE)
          throw timeoutException(response.error.exception);
        response.error.maybeThrow();
        response.node;
    }
  }

  public void  listGroups(Node node): List<GroupOverview> = {
    val response = send(node, ApiKeys.LIST_GROUPS, new ListGroupsRequest.Builder()).asInstanceOf<ListGroupsResponse>;
    response.error.maybeThrow();
    response.groups.asScala.map(group => GroupOverview(group.groupId, group.protocolType)).toList;
  }

  public void  getApiVersions(Node node): List<ApiVersion> = {
    val response = send(node, ApiKeys.API_VERSIONS, new ApiVersionsRequest.Builder()).asInstanceOf<ApiVersionsResponse>;
    response.error.maybeThrow();
    response.apiVersions.asScala.toList;
  }

  /**
   * Wait until there is a non-empty list of brokers in the cluster.
   */
  public void  awaitBrokers() {
    var nodes = List<Node>();
    do {
      nodes = findAllBrokers();
      if (nodes.isEmpty)
        Thread.sleep(50);
    } while (nodes.isEmpty);
  }

  public void  findAllBrokers(): List<Node> = {
    val request = MetadataRequest.Builder.allTopics();
    val response = sendAnyNode(ApiKeys.METADATA, request).asInstanceOf<MetadataResponse>;
    val errors = response.errors;
    if (!errors.isEmpty)
      debug(s"Metadata request contained errors: $errors");
    response.cluster.nodes.asScala.toList;
  }

  public void  listAllGroups(): Map<Node, List[GroupOverview]> = {
    findAllBrokers.map { broker =>
      broker -> {
        try {
          listGroups(broker);
        } catch {
          case Exception e =>
            debug(s"Failed to find groups from broker $broker", e);
            List<GroupOverview>();
        }
      }
    }.toMap;
  }

  public void  listAllConsumerGroups(): Map<Node, List[GroupOverview]> = {
    listAllGroups().mapValues { groups =>
      groups.filter(_.protocolType == ConsumerProtocol.PROTOCOL_TYPE);
    }
  }

  public void  listAllGroupsFlattened(): List<GroupOverview> = {
    listAllGroups.values.flatten.toList;
  }

  public void  listAllConsumerGroupsFlattened(): List<GroupOverview> = {
    listAllGroupsFlattened.filter(_.protocolType == ConsumerProtocol.PROTOCOL_TYPE);
  }

  public void  listGroupOffsets(String groupId): Map<TopicPartition, Long> = {
    val coordinator = findCoordinator(groupId);
    val responseBody = send(coordinator, ApiKeys.OFFSET_FETCH, OffsetFetchRequest.Builder.allTopicPartitions(groupId));
    val response = responseBody.asInstanceOf<OffsetFetchResponse>;
    if (response.hasError)
      throw response.error.exception;
    response.maybeThrowFirstPartitionError;
    response.responseData.asScala.map { case (tp, partitionData) => (tp, partitionData.offset) }.toMap;
  }

  public void  listAllBrokerVersionInfo(): Map<Node, Try[NodeApiVersions]> =
    findAllBrokers.map { broker =>
      broker -> Try<NodeApiVersions>(new NodeApiVersions(getApiVersions(broker).asJava));
    }.toMap;

  /*
   * Remove all the messages whose offset is smaller than the given offset of the corresponding partition
   *
   * DeleteRecordsResult contains either lowWatermark of the partition or exception. We list the possible exception
   * and their interpretations below:
   *
   * - DisconnectException if leader node of the partition is not available. Need retry by user.
   * - PolicyViolationException if the topic is configured as non-deletable.
   * - TopicAuthorizationException if the topic doesn't exist and the user doesn't have the authority to create the topic
   * - TimeoutException if response is not available within the timeout specified by either Future's timeout or AdminClient's request timeout
   * - UnknownTopicOrPartitionException if the partition doesn't exist or if the user doesn't have the authority to describe the topic
   * - NotLeaderForPartitionException if broker is not leader of the partition. Need retry by user.
   * - OffsetOutOfRangeException if the offset is larger than high watermark of this partition
   *
   */

  public void  deleteRecordsBefore(Map offsets<TopicPartition, Long>): Future<Map[TopicPartition, DeleteRecordsResult]> = {
    val metadataRequest = new MetadataRequest.Builder(offsets.keys.map(_.topic).toSet.toList.asJava, true);
    val response = sendAnyNode(ApiKeys.METADATA, metadataRequest).asInstanceOf<MetadataResponse>;
    val errors = response.errors;
    if (!errors.isEmpty)
      error(s"Metadata request contained errors: $errors");

    val (partitionsWithoutError, partitionsWithError) = offsets.partition{ partitionAndOffset =>
      !response.errors().containsKey(partitionAndOffset._1.topic())}

    val (partitionsWithLeader, partitionsWithoutLeader) = partitionsWithoutError.partition{ partitionAndOffset =>
      response.cluster().leaderFor(partitionAndOffset._1) != null}

    val partitionsWithErrorResults = partitionsWithError.keys.map( partition =>
      partition -> DeleteRecordsResult(DeleteRecordsResponse.INVALID_LOW_WATERMARK, response.errors().get(partition.topic()).exception())).toMap;

    val partitionsWithoutLeaderResults = partitionsWithoutLeader.mapValues( _ =>
      DeleteRecordsResult(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.LEADER_NOT_AVAILABLE.exception()));

    val partitionsGroupByLeader = partitionsWithLeader.groupBy(partitionAndOffset =>
      response.cluster().leaderFor(partitionAndOffset._1));

    // prepare requests and generate Future objects;
    val futures = partitionsGroupByLeader.map{ case (node, partitionAndOffsets) =>
      val java convertedMap.util.Map<TopicPartition, java.lang.Long> = partitionAndOffsets.mapValues(_.asInstanceOf<java.lang.Long>).asJava;
      val future = client.send(node, new DeleteRecordsRequest.Builder(requestTimeoutMs, convertedMap));
      pendingFutures.add(future);
      future.compose(new RequestFutureAdapter[ClientResponse, Map<TopicPartition, DeleteRecordsResult]>() {
          override public void  onSuccess(ClientResponse response, RequestFuture future<Map[TopicPartition, DeleteRecordsResult]>) {
            val deleteRecordsResponse = response.responseBody().asInstanceOf<DeleteRecordsResponse>;
            val result = deleteRecordsResponse.responses().asScala.mapValues(v => DeleteRecordsResult(v.lowWatermark, v.error.exception())).toMap;
            future.complete(result);
            pendingFutures.remove(future);
          }

          override public void  onFailure(RuntimeException e, RequestFuture future<Map[TopicPartition, DeleteRecordsResult]>) {
            val result = partitionAndOffsets.mapValues(_ => DeleteRecordsResult(DeleteRecordsResponse.INVALID_LOW_WATERMARK, e));
            future.complete(result);
            pendingFutures.remove(future);
          }

        });
    }

    // default output if not receiving DeleteRecordsResponse before timeout;
    val defaultResults = offsets.mapValues(_ =>
      DeleteRecordsResult(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.REQUEST_TIMED_OUT.exception())) ++ partitionsWithErrorResults ++ partitionsWithoutLeaderResults;

    new CompositeFuture(time, defaultResults, futures.toList);
  }

  /**
   * Case class used to represent a consumer of a consumer group
   */
  case class ConsumerSummary(String consumerId,
                             String clientId,
                             String host,
                             List assignment<TopicPartition>);

  /**
   * Case class used to represent group metadata (including the group coordinator) for the DescribeGroup API
   */
  case class ConsumerGroupSummary(String state,
                                  String assignmentStrategy,
                                  Option consumers<List[ConsumerSummary]>,
                                  Node coordinator);

  public void  describeConsumerGroupHandler(Node coordinator, String groupId): GroupMetadata = {
    val responseBody = send(coordinator, ApiKeys.DESCRIBE_GROUPS,
        new DescribeGroupsRequest.Builder(Collections.singletonList(groupId)));
    val response = responseBody.asInstanceOf<DescribeGroupsResponse>;
    val metadata = response.groups.get(groupId);
    if (metadata == null)
      throw new KafkaException(s"Response from broker contained no metadata for group $groupId")
    metadata;
  }

  public void  describeConsumerGroup(String groupId, Long timeoutMs = 0): ConsumerGroupSummary = {

    public void  isValidConsumerGroupResponse(DescribeGroupsResponse metadata.GroupMetadata): Boolean =
      metadata.error == Errors.NONE && (metadata.state == "Dead" || metadata.state == "Empty" || metadata.protocolType == ConsumerProtocol.PROTOCOL_TYPE);

    val startTime = time.milliseconds;
    val coordinator = findCoordinator(groupId, timeoutMs);
    var metadata = describeConsumerGroupHandler(coordinator, groupId);

    while (!isValidConsumerGroupResponse(metadata) && time.milliseconds - startTime < timeoutMs) {
      debug(s"The consumer group response for group '$groupId' is invalid. Retrying the request as the group is initializing ...")
      Thread.sleep(retryBackoffMs);
      metadata = describeConsumerGroupHandler(coordinator, groupId);
    }

    if (!isValidConsumerGroupResponse(metadata))
      throw new TimeoutException("The consumer group command timed out while waiting for group to initialize")

    val consumers = metadata.members.asScala.map { consumer =>
      ConsumerSummary(consumer.memberId, consumer.clientId, consumer.clientHost, metadata.state match {
        case "Stable" =>
          val assignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(Utils.readBytes(consumer.memberAssignment)));
          assignment.partitions.asScala.toList;
        case _ =>
          List();
      });
    }.toList;

    ConsumerGroupSummary(metadata.state, metadata.protocol, Some(consumers), coordinator);
  }

  public void  close() {
    running = false;
    try {
      client.close();
    } catch {
      case IOException e =>
        error("Exception closing nioSelector:", e);
    }
  }

}

/*
 * CompositeFuture assumes that the future object in the futures list does not raise error
 */
class CompositeFuture[T](Time time,
                         Map defaultResults<TopicPartition, T>,
                         List futures<RequestFuture[Map[TopicPartition, T]]>) extends Future<Map[TopicPartition, T]> {

  override public void  isCancelled = false;

  override public void  cancel(Boolean interrupt) = false;

  override public void  get(): Map<TopicPartition, T> = {
    get(Long.MaxValue, TimeUnit.MILLISECONDS);
  }

  override public void  get(Long timeout, TimeUnit unit): Map<TopicPartition, T> = {
    val Long start = time.milliseconds();
    val timeoutMs = unit.toMillis(timeout);
    var Long remaining = timeoutMs;

    val observedResults = futures.flatMap{ future =>
      val elapsed = time.milliseconds() - start;
      remaining = if (timeoutMs - elapsed > 0) timeoutMs - elapsed else 0L;

      if (future.awaitDone(remaining, TimeUnit.MILLISECONDS)) future.value()
      else Map.empty<TopicPartition, T>;
    }.toMap;

    defaultResults ++ observedResults;
  }

  override public void  Boolean isDone = {
    futures.forall(_.isDone)
  }
}

object AdminClient {
  val DefaultConnectionMaxIdleMs = 9 * 60 * 1000;
  val DefaultRequestTimeoutMs = 5000;
  val DefaultMaxInFlightRequestsPerConnection = 100;
  val DefaultReconnectBackoffMs = 50;
  val DefaultReconnectBackoffMax = 50;
  val DefaultSendBufferBytes = 128 * 1024;
  val DefaultReceiveBufferBytes = 32 * 1024;
  val DefaultRetryBackoffMs = 100;

  val AdminClientIdSequence = new AtomicInteger(1);
  val AdminConfigDef = {
    val config = new ConfigDef();
      .define(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        Type.LIST,
        Importance.HIGH,
        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC);
      .define(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        ConfigDef.Type.STRING,
        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
        ConfigDef.Importance.MEDIUM,
        CommonClientConfigs.SECURITY_PROTOCOL_DOC);
      .define(
        CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
        ConfigDef.Type.INT,
        DefaultRequestTimeoutMs,
        ConfigDef.Importance.MEDIUM,
        CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC);
      .define(
        CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
        ConfigDef.Type.LONG,
        DefaultRetryBackoffMs,
        ConfigDef.Importance.MEDIUM,
        CommonClientConfigs.RETRY_BACKOFF_MS_DOC);
      .withClientSslSupport();
      .withClientSaslSupport();
    config;
  }

  case class DeleteRecordsResult(Long lowWatermark, Exception error);

  class AdminConfig(Map originals<_,_>) extends AbstractConfig(AdminConfigDef, originals.asJava, false);

  public void  createSimplePlaintext(String brokerUrl): AdminClient = {
    val config = Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> brokerUrl);
    create(new AdminConfig(config));
  }

  public void  create(Properties props): AdminClient = create(props.asScala.toMap);

  public void  create(Map props<String, _>): AdminClient = create(new AdminConfig(props));

  public void  create(AdminConfig config): AdminClient = {
    val time = Time.SYSTEM;
    val metrics = new Metrics(time);
    val metadata = new Metadata(100L, 60 * 60 * 1000L, true);
    val channelBuilder = ClientUtils.createChannelBuilder(config);
    val requestTimeoutMs = config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG);
    val retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);

    val brokerUrls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    val brokerAddresses = ClientUtils.parseAndValidateAddresses(brokerUrls);
    val bootstrapCluster = Cluster.bootstrap(brokerAddresses);
    metadata.update(bootstrapCluster, Collections.emptySet(), 0);

    val selector = new Selector(
      DefaultConnectionMaxIdleMs,
      metrics,
      time,
      "admin",
      channelBuilder);

    val networkClient = new NetworkClient(
      selector,
      metadata,
      "admin-" + AdminClientIdSequence.getAndIncrement(),
      DefaultMaxInFlightRequestsPerConnection,
      DefaultReconnectBackoffMs,
      DefaultReconnectBackoffMax,
      DefaultSendBufferBytes,
      DefaultReceiveBufferBytes,
      requestTimeoutMs,
      time,
      true,
      new ApiVersions);

    val highLevelClient = new ConsumerNetworkClient(
      networkClient,
      metadata,
      time,
      retryBackoffMs,
      requestTimeoutMs.toLong);

    new AdminClient(
      time,
      requestTimeoutMs,
      retryBackoffMs,
      highLevelClient,
      bootstrapCluster.nodes.asScala.toList);
  }
}
