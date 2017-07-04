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

import kafka.server.{AbstractFetcherManager, AbstractFetcherThread, BrokerAndInitialOffset}
import kafka.cluster.{BrokerEndPoint, Cluster}
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import scala.collection.immutable;
import collection.mutable.HashMap;
import scala.collection.mutable;
import java.util.concurrent.locks.ReentrantLock;

import kafka.utils.CoreUtils.inLock;
import kafka.utils.ZkUtils;
import kafka.utils.ShutdownableThread;
import kafka.client.ClientUtils;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *  Usage:
 *  Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 *  until shutdown() is called.
 */
@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class ConsumerFetcherManager(private val String consumerIdString,
                             private val ConsumerConfig config,
                             private val zkUtils : ZkUtils);
        extends AbstractFetcherManager(String.format("ConsumerFetcherManager-%d",Time.SYSTEM.milliseconds),
                                       config.clientId, config.numConsumerFetchers) {
  private var immutable partitionMap.Map<TopicPartition, PartitionTopicInfo> = null;
  private var Cluster cluster = null;
  private val noLeaderPartitionSet = new mutable.HashSet<TopicPartition>;
  private val lock = new ReentrantLock;
  private val cond = lock.newCondition();
  private var ShutdownableThread leaderFinderThread = null;
  private val correlationId = new AtomicInteger(0);

  private class LeaderFinderThread(String name) extends ShutdownableThread(name) {
    // thread responsible for adding the fetcher to the right broker when leader is available;
    override public void  doWork() {
      val leaderForPartitionsMap = new HashMap<TopicPartition, BrokerEndPoint>;
      lock.lock();
      try {
        while (noLeaderPartitionSet.isEmpty) {
          trace("No partition for leader election.")
          cond.await();
        }

        trace(String.format("Partitions without leader %s",noLeaderPartitionSet))
        val brokers = ClientUtils.getPlaintextBrokerEndPoints(zkUtils);
        val topicsMetadata = ClientUtils.fetchTopicMetadata(noLeaderPartitionSet.map(m => m.topic).toSet,
                                                            brokers,
                                                            config.clientId,
                                                            config.socketTimeoutMs,
                                                            correlationId.getAndIncrement).topicsMetadata;
        if(logger.isDebugEnabled) topicsMetadata.foreach(topicMetadata => debug(topicMetadata.toString()))
        topicsMetadata.foreach { tmd =>
          val topic = tmd.topic;
          tmd.partitionsMetadata.foreach { pmd =>
            val topicAndPartition = new TopicPartition(topic, pmd.partitionId);
            if(pmd.leader.isDefined && noLeaderPartitionSet.contains(topicAndPartition)) {
              val leaderBroker = pmd.leader.get;
              leaderForPartitionsMap.put(topicAndPartition, leaderBroker);
              noLeaderPartitionSet -= topicAndPartition;
            }
          }
        }
      } catch {
        case Throwable t => {
            if (!isRunning.get())
              throw t /* If this thread is stopped, propagate this exception to kill the thread. */
            else;
              warn(String.format("Failed to find leader for %s",noLeaderPartitionSet), t)
          }
      } finally {
        lock.unlock();
      }

      try {
        addFetcherForPartitions(leaderForPartitionsMap.map { case (topicPartition, broker) =>
          topicPartition -> BrokerAndInitialOffset(broker, partitionMap(topicPartition).getFetchOffset())}
        );
      } catch {
        case Throwable t =>
          if (!isRunning.get())
            throw t /* If this thread is stopped, propagate this exception to kill the thread. */
          else {
            warn(String.format("Failed to add leader for partitions %s; will retry",leaderForPartitionsMap.keySet.mkString(",")), t)
            lock.lock();
            noLeaderPartitionSet ++= leaderForPartitionsMap.keySet;
            lock.unlock();
          }
        }

      shutdownIdleFetcherThreads();
      Thread.sleep(config.refreshLeaderBackoffMs);
    }
  }

  override public void  createFetcherThread Integer fetcherId, BrokerEndPoint sourceBroker): AbstractFetcherThread = {
    new ConsumerFetcherThread(
      String.format("ConsumerFetcherThread-%s-%d-%d",consumerIdString, fetcherId, sourceBroker.id),
      config, sourceBroker, partitionMap, this);
  }

  public void  startConnections(Iterable topicInfos<PartitionTopicInfo>, Cluster cluster) {
    leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread");
    leaderFinderThread.start();

    inLock(lock) {
      partitionMap = topicInfos.map(tpi => (new TopicPartition(tpi.topic, tpi.partitionId), tpi)).toMap;
      this.cluster = cluster;
      noLeaderPartitionSet ++= topicInfos.map(tpi => new TopicPartition(tpi.topic, tpi.partitionId));
      cond.signalAll();
    }
  }

  public void  stopConnections() {
    /*
     * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
     * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
     * these partitions.
     */
    info("Stopping leader finder thread");
    if (leaderFinderThread != null) {
      leaderFinderThread.shutdown();
      leaderFinderThread = null;
    }

    info("Stopping all fetchers");
    closeAllFetchers();

    // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped;
    partitionMap = null;
    noLeaderPartitionSet.clear();

    info("All connections stopped");
  }

  public void  addPartitionsWithError(Iterable partitionList<TopicPartition>) {
    debug(String.format("adding partitions with error %s",partitionList))
    inLock(lock) {
      if (partitionMap != null) {
        noLeaderPartitionSet ++= partitionList;
        cond.signalAll();
      }
    }
  }
}
