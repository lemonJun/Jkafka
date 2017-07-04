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

import kafka.api.{FetchRequestBuilder, FetchResponsePartitionData, OffsetRequest, Request}
import kafka.cluster.BrokerEndPoint;
import kafka.message.ByteBufferMessageSet;
import kafka.server.{AbstractFetcherThread, PartitionFetchState}
import kafka.common.{ErrorMapping, TopicAndPartition}

import scala.collection.Map;
import ConsumerFetcherThread._;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.EpochEndOffset;

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.internals.Fetcher instead.", "0.11.0.0");
class ConsumerFetcherThread(String name,
                            val ConsumerConfig config,
                            BrokerEndPoint sourceBroker,
                            Map partitionMap<TopicPartition, PartitionTopicInfo>,
                            val ConsumerFetcherManager consumerFetcherManager);
        extends AbstractFetcherThread(name = name,
                                      clientId = config.clientId,
                                      sourceBroker = sourceBroker,
                                      fetchBackOffMs = config.refreshLeaderBackoffMs,
                                      isInterruptible = true,
                                      includeLogTruncation = false) {

  type REQ = FetchRequest;
  type PD = PartitionData;

  private val clientId = config.clientId;
  private val fetchSize = config.fetchMessageMaxBytes;

  private val simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, config.socketTimeoutMs,
    config.socketReceiveBufferBytes, config.clientId);

  private val fetchRequestBuilder = new FetchRequestBuilder().;
    clientId(clientId).;
    replicaId(Request.OrdinaryConsumerId).;
    maxWait(config.fetchWaitMaxMs).;
    minBytes(config.fetchMinBytes).;
    requestVersion(3) // for now, the old consumer is pinned to the old message format through the fetch request;

  override public void  initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown();
    if (justShutdown && isInterruptible)
      simpleConsumer.disconnectToHandleJavaIOBug();
    justShutdown;
  }

  override public void  shutdown(): Unit = {
    super.shutdown();
    simpleConsumer.close();
  }

  // process fetched data;
  public void  processPartitionData(TopicPartition topicPartition, Long fetchOffset, PartitionData partitionData) {
    val pti = partitionMap(topicPartition);
    if (pti.getFetchOffset != fetchOffset)
      throw new RuntimeException("Offset doesn't match for partition <%s,%d> pti offset: %d fetch offset: %d";
                                .format(topicPartition.topic, topicPartition.partition, pti.getFetchOffset, fetchOffset))
    pti.enqueue(partitionData.underlying.messages.asInstanceOf<ByteBufferMessageSet>);
  }

  // handle a partition whose offset is out of range and return a new fetch offset;
  public void  handleOffsetOutOfRange(TopicPartition topicPartition): Long = {
    val startTimestamp = config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => OffsetRequest.EarliestTime;
      case _ => OffsetRequest.LatestTime;
    }
    val topicAndPartition = TopicAndPartition(topicPartition.topic, topicPartition.partition);
    val newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Request.OrdinaryConsumerId);
    val pti = partitionMap(topicPartition);
    pti.resetFetchOffset(newOffset);
    pti.resetConsumeOffset(newOffset);
    newOffset;
  }

  // any logic for partitions whose leader has changed;
  public void  handlePartitionsWithErrors(Iterable partitions<TopicPartition>) {
    removePartitions(partitions.toSet);
    consumerFetcherManager.addPartitionsWithError(partitions);
  }

  protected public void  buildFetchRequest(collection partitionMap.Seq<(TopicPartition, PartitionFetchState)>): FetchRequest = {
    partitionMap.foreach { case ((topicPartition, partitionFetchState)) =>
      if (partitionFetchState.isReadyForFetch)
        fetchRequestBuilder.addFetch(topicPartition.topic, topicPartition.partition, partitionFetchState.fetchOffset, fetchSize);
    }

    new FetchRequest(fetchRequestBuilder.build());
  }

  protected public void  fetch(FetchRequest fetchRequest): Seq<(TopicPartition, PartitionData)> =
    simpleConsumer.fetch(fetchRequest.underlying).data.map { case (TopicAndPartition(t, p), value) =>
      new TopicPartition(t, p) -> new PartitionData(value);
    }

  override public void  buildLeaderEpochRequest(Seq allPartitions<(TopicPartition, PartitionFetchState)>): Map<TopicPartition, Int> = { Map() }

  override public void  fetchEpochsFromLeader(Map partitions<TopicPartition, Int>): Map<TopicPartition, EpochEndOffset> = { Map() }

  override public void  maybeTruncate(Map fetchedEpochs<TopicPartition, EpochEndOffset>): Map<TopicPartition, Long> = { Map() }
}

@deprecated("This object has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.internals.Fetcher instead.", "0.11.0.0");
object ConsumerFetcherThread {

  class FetchRequest(val kafka underlying.api.FetchRequest) extends AbstractFetcherThread.FetchRequest {
    private lazy val Map tpToOffset<TopicPartition, Long> = underlying.requestInfo.map { case (tp, fetchInfo) =>
      new TopicPartition(tp.topic, tp.partition) -> fetchInfo.offset;
    }.toMap;
    public void  Boolean isEmpty = underlying.requestInfo.isEmpty;
    public void  offset(TopicPartition topicPartition): Long = tpToOffset(topicPartition);
    override public void  toString = underlying.toString;
  }

  class PartitionData(val FetchResponsePartitionData underlying) extends AbstractFetcherThread.PartitionData {
    public void  error = underlying.error;
    public void  MemoryRecords toRecords = underlying.messages.asInstanceOf<ByteBufferMessageSet>.asRecords;
    public void  Long highWatermark = underlying.hw;
    public void  Option exception<Throwable> =
      if (error == Errors.NONE) None else Some(ErrorMapping.exceptionFor(error.code))
    override public void  toString = underlying.toString;
  }
}
