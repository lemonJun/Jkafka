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

package kafka.consumer;


import java.nio.channels.{AsynchronousCloseException, ClosedByInterruptException}
import java.util.concurrent.TimeUnit;

import kafka.api._;
import kafka.network._;
import kafka.utils._;
import kafka.common.{ErrorMapping, TopicAndPartition}
import org.apache.kafka.common.network.{NetworkReceive}
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils._;

/**
 * A consumer of kafka messages
 */
@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.KafkaConsumer instead.", "0.11.0.0");
@threadsafe
class SimpleConsumer(val String host,
                     val Integer port,
                     val Integer soTimeout,
                     val Integer bufferSize,
                     val String clientId) extends Logging {

  ConsumerConfig.validateClientId(clientId);
  private val lock = new Object();
  private val blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout);
  private val fetchRequestAndResponseStats = FetchRequestAndResponseStatsRegistry.getFetchRequestAndResponseStats(clientId);
  private var isClosed = false;

  private public void  connect(): BlockingChannel = {
    close;
    blockingChannel.connect();
    blockingChannel;
  }

  private public void  disconnect() = {
    debug("Disconnecting from " + formatAddress(host, port))
    blockingChannel.disconnect();
  }

  private public void  reconnect() {
    disconnect();
    connect();
  }

  /**
   * Unblock thread by closing channel and triggering AsynchronousCloseException if a read operation is in progress.
   *
   * This handles a bug found in Java 1.7 and below, where interrupting a thread can not correctly unblock
   * the thread from waiting on ReadableByteChannel.read().
   */
  public void  disconnectToHandleJavaIOBug() = {
    disconnect();
  }

  public void  close() {
    lock synchronized {
      disconnect();
      isClosed = true;
    }
  }

  private public void  sendRequest(RequestOrResponse request): NetworkReceive = {
    lock synchronized {
      var NetworkReceive response = null;
      try {
        getOrMakeConnection();
        blockingChannel.send(request);
        response = blockingChannel.receive();
      } catch {
        case e : ClosedByInterruptException =>
          throw e;
        // Should not observe this exception when running Kafka with Java 1.8;
        case AsynchronousCloseException e =>
          throw e;
        case e : Throwable =>
          info("Reconnect due to error:", e);
          // retry once;
          try {
            reconnect();
            blockingChannel.send(request);
            response = blockingChannel.receive();
          } catch {
            case Throwable e =>
              disconnect();
              throw e;
          }
      }
      response;
    }
  }

  public void  send(TopicMetadataRequest request): TopicMetadataResponse = {
    val response = sendRequest(request);
    TopicMetadataResponse.readFrom(response.payload());
  }

  public void  send(GroupCoordinatorRequest request): GroupCoordinatorResponse = {
    val response = sendRequest(request);
    GroupCoordinatorResponse.readFrom(response.payload());
  }

  /**
   *  Fetch a set of messages from a topic.
   *
   *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
   *  @return a set of fetched messages
   */
  public void  fetch(FetchRequest request): FetchResponse = {
    var NetworkReceive response = null;
    val specificTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).requestTimer;
    val aggregateTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.requestTimer;
    aggregateTimer.time {
      specificTimer.time {
        response = sendRequest(request);
      }
    }
    val fetchResponse = FetchResponse.readFrom(response.payload(), request.versionId);
    val fetchedSize = fetchResponse.sizeInBytes;
    fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).requestSizeHist.update(fetchedSize);
    fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.requestSizeHist.update(fetchedSize);
    fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).throttleTimeStats.update(fetchResponse.throttleTimeMs, TimeUnit.MILLISECONDS);
    fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.throttleTimeStats.update(fetchResponse.throttleTimeMs, TimeUnit.MILLISECONDS);
    fetchResponse;
  }

  /**
   *  Get a list of valid offsets (up to maxSize) before the given time.
   *  @param request a <[kafka.api.OffsetRequest]> object.
   *  @return a <[kafka.api.OffsetResponse]> object.
   */
  public void  getOffsetsBefore(OffsetRequest request) = OffsetResponse.readFrom(sendRequest(request).payload())

  /**
   * Commit offsets for a topic
   * Version 0 of the request will commit offsets to Zookeeper and version 1 and above will commit offsets to Kafka.
   * @param request a <[kafka.api.OffsetCommitRequest]> object.
   * @return a <[kafka.api.OffsetCommitResponse]> object.
   */
  public void  commitOffsets(OffsetCommitRequest request) = {
    // With TODO KAFKA-1012, we have to first issue a ConsumerMetadataRequest and connect to the coordinator before;
    // we can commit offsets.;
    OffsetCommitResponse.readFrom(sendRequest(request).payload());
  }

  /**
   * Fetch offsets for a topic
   * Version 0 of the request will fetch offsets from Zookeeper and version 1 and above will fetch offsets from Kafka.
   * @param request a <[kafka.api.OffsetFetchRequest]> object.
   * @return a <[kafka.api.OffsetFetchResponse]> object.
   */
  public void  fetchOffsets(OffsetFetchRequest request) = OffsetFetchResponse.readFrom(sendRequest(request).payload(), request.versionId);

  private public void  getOrMakeConnection() {
    if(!isClosed && !blockingChannel.isConnected) {
      connect();
    }
  }

  /**
   * Get the earliest or latest offset of a given topic, partition.
   * @param topicAndPartition Topic and partition of which the offset is needed.
   * @param earliestOrLatest A value to indicate earliest or latest offset.
   * @param consumerId Id of the consumer which could be a consumer client, SimpleConsumerShell or a follower broker.
   * @return Requested offset.
   */
  public void  earliestOrLatestOffset(TopicAndPartition topicAndPartition, Long earliestOrLatest, Integer consumerId): Long = {
    val request = OffsetRequest(requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(earliestOrLatest, 1)),
                                clientId = clientId,
                                replicaId = consumerId);
    val partitionErrorAndOffset = getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition)
    val offset = partitionErrorAndOffset.error match {
      case Errors.NONE => partitionErrorAndOffset.offsets.head;
      case _ => throw ErrorMapping.exceptionFor(partitionErrorAndOffset.error.code);
    }
    offset;
  }
}

