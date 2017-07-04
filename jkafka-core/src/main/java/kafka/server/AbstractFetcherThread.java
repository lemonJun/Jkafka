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

package kafka.server;

import java.util.concurrent.locks.ReentrantLock;

import kafka.cluster.BrokerEndPoint;
import kafka.consumer.PartitionTopicInfo;
import kafka.utils.{DelayedItem, Pool, ShutdownableThread}
import kafka.common.{ClientIdAndBroker, KafkaException}
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.CoreUtils.inLock;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.protocol.Errors;
import AbstractFetcherThread._;

import scala.collection.{Map, Set, mutable}
import scala.collection.JavaConverters._;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.yammer.metrics.core.Gauge;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.{FatalExitError, PartitionStates}
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.EpochEndOffset;

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(String name,
                                     String clientId,
                                     val BrokerEndPoint sourceBroker,
                                     Integer fetchBackOffMs = 0,
                                     Boolean isInterruptible = true,
                                     Boolean includeLogTruncation;
                                    );
  extends ShutdownableThread(name, isInterruptible) {

  type REQ <: FetchRequest;
  type PD <: PartitionData;

  private<server> val partitionStates = new PartitionStates<PartitionFetchState>;
  private val partitionMapLock = new ReentrantLock;
  private val partitionMapCond = partitionMapLock.newCondition();

  private val metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port);
  val fetcherStats = new FetcherStats(metricId);
  val fetcherLagStats = new FetcherLagStats(metricId);

  /* callbacks to be defined in subclass */

  // process fetched data;
  protected public void  processPartitionData(TopicPartition topicPartition, Long fetchOffset, PD partitionData);

  // handle a partition whose offset is out of range and return a new fetch offset;
  protected public void  handleOffsetOutOfRange(TopicPartition topicPartition): Long;

  // deal with partitions with errors, potentially due to leadership changes;
  protected public void  handlePartitionsWithErrors(Iterable partitions<TopicPartition>);

  protected public void  buildLeaderEpochRequest(Seq allPartitions<(TopicPartition, PartitionFetchState)>): Map<TopicPartition, Int>;

  protected public void  fetchEpochsFromLeader(Map partitions<TopicPartition, Int>): Map<TopicPartition, EpochEndOffset>;

  protected public void  maybeTruncate(Map fetchedEpochs<TopicPartition, EpochEndOffset>): Map<TopicPartition, Long>;

  protected public void  buildFetchRequest(Seq partitionMap<(TopicPartition, PartitionFetchState)>): REQ;

  protected public void  fetch(REQ fetchRequest): Seq<(TopicPartition, PD)>;

  override public void  shutdown(){
    initiateShutdown();
    inLock(partitionMapLock) {
      partitionMapCond.signalAll();
    }
    awaitShutdown();

    // we don't need the lock since the thread has finished shutdown and metric removal is safe;
    fetcherStats.unregister();
    fetcherLagStats.unregister();
  }

  private public void  states() = partitionStates.partitionStates.asScala.map { state => state.topicPartition -> state.value }

  override public void  doWork() {
    maybeTruncate();
    val fetchRequest = inLock(partitionMapLock) {
      val fetchRequest = buildFetchRequest(states);
      if (fetchRequest.isEmpty) {
        trace(String.format("There are no active partitions. Back off for %d ms before sending a fetch request",fetchBackOffMs))
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS);
      }
      fetchRequest;
    }
    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest);
  }

  /**
    * - Build a leader epoch fetch based on partitions that are in the Truncating phase
    * - Issue LeaderEpochRequeust, retrieving the latest offset for each partition's
    *   leader epoch. This is the offset the follower should truncate to ensure
    *   accurate log replication.
    * - Finally truncate the logs for partitions in the truncating phase and mark them
    *   truncation complete. Do this within a lock to ensure no leadership changes can
    *   occur during truncation.
    */
  public void  maybeTruncate(): Unit = {
    val epochRequests = inLock(partitionMapLock) { buildLeaderEpochRequest(states) }

    if (!epochRequests.isEmpty) {
      val fetchedEpochs = fetchEpochsFromLeader(epochRequests);
      //Ensure we hold a lock during truncation.;
      inLock(partitionMapLock) {
        //Check no leadership changes happened whilst we were unlocked, fetching epochs;
        val leaderEpochs = fetchedEpochs.filter { case (tp, _) => partitionStates.contains(tp) }
        val truncationPoints = maybeTruncate(leaderEpochs);
        markTruncationComplete(truncationPoints);
      }
    }
  }

  private public void  processFetchRequest(REQ fetchRequest) {
    val partitionsWithError = mutable.Set<TopicPartition>();

    public void  updatePartitionsWithError(TopicPartition partition): Unit = {
      partitionsWithError += partition;
      partitionStates.moveToEnd(partition);
    }

    var Seq responseData<(TopicPartition, PD)> = Seq.empty;

    try {
      trace(s"Issuing fetch to broker ${sourceBroker.id}, request: $fetchRequest");
      responseData = fetch(fetchRequest);
    } catch {
      case Throwable t =>
        if (isRunning.get) {
          warn(s"Error in fetch to broker ${sourceBroker.id}, request ${fetchRequest}", t);
          inLock(partitionMapLock) {
            partitionStates.partitionSet.asScala.foreach(updatePartitionsWithError)
            // there is an error occurred while fetching partitions, sleep a while;
            // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every;
            // partition with error effectively doubling the delay. It would be good to improve this.;
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS);
          }
        }
    }
    fetcherStats.requestRate.mark();

    if (responseData.nonEmpty) {
      // process fetched data;
      inLock(partitionMapLock) {

        responseData.foreach { case (topicPartition, partitionData) =>
          val topic = topicPartition.topic;
          val partitionId = topicPartition.partition;
          Option(partitionStates.stateValue(topicPartition)).foreach(currentPartitionFetchState =>
            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch;
            if (fetchRequest.offset(topicPartition) == currentPartitionFetchState.fetchOffset) {
              partitionData.error match {
                case Errors.NONE =>
                  try {
                    val records = partitionData.toRecords;
                    val newOffset = records.batches.asScala.lastOption.map(_.nextOffset).getOrElse(
                      currentPartitionFetchState.fetchOffset);

                    fetcherLagStats.getAndMaybePut(topic, partitionId).lag = Math.max(0L, partitionData.highWatermark - newOffset);
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread;
                    processPartitionData(topicPartition, currentPartitionFetchState.fetchOffset, partitionData);

                    val validBytes = records.validBytes;
                    if (validBytes > 0) {
                      // Update partitionStates only if there is no exception during processPartitionData;
                      partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset));
                      fetcherStats.byteRate.mark(validBytes);
                    }
                  } catch {
                    case CorruptRecordException ime =>
                      // we log the error and continue. This ensures two things;
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag;
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and;
                      // should get fixed in the subsequent fetches;
                      logger.error("Found invalid messages during fetch for partition [" + topic + "," + partitionId + "] offset " + currentPartitionFetchState.fetchOffset  + " error " + ime.getMessage)
                      updatePartitionsWithError(topicPartition);
                    case Throwable e =>
                      throw new KafkaException("error processing data for partition <%s,%d> offset %d";
                        .format(topic, partitionId, currentPartitionFetchState.fetchOffset), e)
                  }
                case Errors.OFFSET_OUT_OF_RANGE =>
                  try {
                    val newOffset = handleOffsetOutOfRange(topicPartition);
                    partitionStates.updateAndMoveToEnd(topicPartition, new PartitionFetchState(newOffset));
                    error("Current offset %d for partition <%s,%d> out of range; reset offset to %d";
                      .format(currentPartitionFetchState.fetchOffset, topic, partitionId, newOffset))
                  } catch {
                    case FatalExitError e => throw e;
                    case Throwable e =>
                      error(String.format("Error getting offset for partition <%s,%d> to broker %d",topic, partitionId, sourceBroker.id), e)
                      updatePartitionsWithError(topicPartition);
                  }
                case _ =>
                  if (isRunning.get) {
                    error(String.format("Error for partition <%s,%d> to broker %d:%s",topic, partitionId, sourceBroker.id,
                      partitionData.exception.get));
                    updatePartitionsWithError(topicPartition);
                  }
              }
            });
        }
      }
    }

    if (partitionsWithError.nonEmpty) {
      debug(String.format("handling partitions with error for %s",partitionsWithError))
      handlePartitionsWithErrors(partitionsWithError);
    }
  }

  public void  addPartitions(Map partitionAndOffsets<TopicPartition, Long>) {
    partitionMapLock.lockInterruptibly();
    try {
      // If the partitionMap already has the topic/partition, then do not update the map with the old offset;
      val newPartitionToState = partitionAndOffsets.filter { case (tp, _) =>
        !partitionStates.contains(tp);
      }.map { case (tp, offset) =>
        val fetchState =
          if (offset < 0)
            new PartitionFetchState(handleOffsetOutOfRange(tp), includeLogTruncation);
          else;
            new PartitionFetchState(offset, includeLogTruncation);
        tp -> fetchState;
      }
      val existingPartitionToState = states().toMap;
      partitionStates.set((existingPartitionToState ++ newPartitionToState).asJava);
      partitionMapCond.signalAll();
    } finally partitionMapLock.unlock();
  }

  /**
    * Loop through all partitions, marking them as truncation complete and applying the correct offset
    * @param partitions the partitions to mark truncation complete
    */
  private public void  markTruncationComplete(Map partitions<TopicPartition, Long>) {
    val Map newStates<TopicPartition, PartitionFetchState> = partitionStates.partitionStates.asScala;
      .map { state =>
        val maybeTruncationComplete = partitions.get(state.topicPartition()) match {
          case Some(offset) => new PartitionFetchState(offset, state.value.delay, truncatingLog = false);
          case None => state.value();
        }
        (state.topicPartition(), maybeTruncationComplete);
      }.toMap;
    partitionStates.set(newStates.asJava);
  }

  public void  delayPartitions(Iterable partitions<TopicPartition>, Long delay) {
    partitionMapLock.lockInterruptibly();
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach (currentPartitionFetchState =>
          if (!currentPartitionFetchState.isDelayed)
            partitionStates.updateAndMoveToEnd(partition, new PartitionFetchState(currentPartitionFetchState.fetchOffset, new DelayedItem(delay), currentPartitionFetchState.truncatingLog));
        );
      }
      partitionMapCond.signalAll();
    } finally partitionMapLock.unlock();
  }

  public void  removePartitions(Set topicPartitions<TopicPartition>) {
    partitionMapLock.lockInterruptibly();
    try {
      topicPartitions.foreach { topicPartition =>
        partitionStates.remove(topicPartition);
        fetcherLagStats.unregister(topicPartition.topic, topicPartition.partition);
      }
    } finally partitionMapLock.unlock();
  }

  public void  partitionCount() = {
    partitionMapLock.lockInterruptibly();
    try partitionStates.size;
    finally partitionMapLock.unlock();
  }

}

object AbstractFetcherThread {

  trait FetchRequest {
    public void  Boolean isEmpty;
    public void  offset(TopicPartition topicPartition): Long;
  }

  trait PartitionData {
    public void  Errors error;
    public void  Option exception<Throwable>;
    public void  MemoryRecords toRecords;
    public void  Long highWatermark;
  }

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag";
  val RequestsPerSec = "RequestsPerSec";
  val BytesPerSec = "BytesPerSec";
}

class FetcherLagMetrics(ClientIdTopicPartition metricId) extends KafkaMetricsGroup {

  private<this> val lagVal = new AtomicLong(-1L);
  private<this> val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topic,
    "partition" -> metricId.partitionId.toString);

  newGauge(FetcherMetrics.ConsumerLag,
    new Gauge<Long> {
      public void  value = lagVal.get;
    },
    tags;
  );

  public void  lag_=(Long newLag) {
    lagVal.set(newLag);
  }

  public void  lag = lagVal.get;

  public void  unregister() {
    removeMetric(FetcherMetrics.ConsumerLag, tags);
  }
}

class FetcherLagStats(ClientIdAndBroker metricId) {
  private val valueFactory = (ClientIdTopicPartition k) => new FetcherLagMetrics(k);
  val stats = new Pool<ClientIdTopicPartition, FetcherLagMetrics>(Some(valueFactory));

  public void  getAndMaybePut(String topic, Integer partitionId): FetcherLagMetrics = {
    stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId));
  }

  public void  isReplicaInSync(String topic, Integer partitionId): Boolean = {
    val fetcherLagMetrics = stats.get(new ClientIdTopicPartition(metricId.clientId, topic, partitionId));
    if (fetcherLagMetrics != null)
      fetcherLagMetrics.lag <= 0;
    else;
      false;
  }

  public void  unregister(String topic, Integer partitionId) {
    val lagMetrics = stats.remove(new ClientIdTopicPartition(metricId.clientId, topic, partitionId));
    if (lagMetrics != null) lagMetrics.unregister()
  }

  public void  unregister() {
    stats.keys.toBuffer.foreach { ClientIdTopicPartition key =>
      unregister(key.topic, key.partitionId);
    }
  }
}

class FetcherStats(ClientIdAndBroker metricId) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString);

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags);

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags);

  public void  unregister() {
    removeMetric(FetcherMetrics.RequestsPerSec, tags);
    removeMetric(FetcherMetrics.BytesPerSec, tags);
  }

}

case class ClientIdTopicPartition(String clientId, String topic, Integer partitionId) {
  override public void  toString = String.format("%s-%s-%d",clientId, topic, partitionId)
}

/**
  * case class to keep partition offset and its state(truncatingLog, delayed)
  * This represents a partition as being either:
  * (1) Truncating its log, for example having recently become a follower
  * (2) Delayed, for example due to an error, where we subsequently back off a bit
  * (3) ReadyForFetch, the is the active state where the thread is actively fetching data.
  */
case class PartitionFetchState(Long fetchOffset, DelayedItem delay, Boolean truncatingLog = false) {

  public void  this(Long offset, Boolean truncatingLog) = this(offset, new DelayedItem(0), truncatingLog);

  public void  this(Long offset, DelayedItem delay) = this(offset, new DelayedItem(0), false);

  public void  this(Long fetchOffset) = this(fetchOffset, new DelayedItem(0));

  public void  Boolean isReadyForFetch = delay.getDelay(TimeUnit.MILLISECONDS) == 0 && !truncatingLog;

  public void  Boolean isTruncatingLog = delay.getDelay(TimeUnit.MILLISECONDS) == 0 && truncatingLog;

  public void  Boolean isDelayed = delay.getDelay(TimeUnit.MILLISECONDS) > 0;

  override public void  toString = String.format("offset:%d-isReadyForFetch:%b-isTruncatingLog:%b",fetchOffset, isReadyForFetch, truncatingLog)
}
