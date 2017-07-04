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

import kafka.network._;
import kafka.utils._;
import kafka.metrics.KafkaMetricsGroup;
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Meter;
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.{Time, Utils}

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler Integer id,
                          Integer brokerId,
                          val Meter aggregateIdleMeter,
                          val Integer totalHandlerThreads,
                          val RequestChannel requestChannel,
                          KafkaApis apis,
                          Time time) extends Runnable with Logging {
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], ";
  private val latch = new CountDownLatch(1);

  public void  run() {
    while (true) {
      try {
        var req : RequestChannel.Request = null;
        while (req == null) {
          // We use a single meter for aggregate idle percentage for the thread pool.;
          // Since meter is calculated as total_recorded_value / time_window and;
          // time_window is independent of the number of threads, each recorded idle;
          // time should be discounted by # threads.;
          val startSelectTime = time.nanoseconds;
          req = requestChannel.receiveRequest(300);
          val endTime = time.nanoseconds;
          if (req != null)
            req.requestDequeueTimeNanos = endTime;
          val idleTime = endTime - startSelectTime;
          aggregateIdleMeter.mark(idleTime / totalHandlerThreads);
        }

        if (req eq RequestChannel.AllDone) {
          debug(String.format("Kafka request handler %d on broker %d received shut down command",id, brokerId))
          latch.countDown();
          return;
        }
        trace(String.format("Kafka request handler %d on broker %d handling request %s",id, brokerId, req))
        apis.handle(req);
      } catch {
        case FatalExitError e =>
          latch.countDown();
          Exit.exit(e.statusCode);
        case Throwable e => error("Exception when handling request", e);
      }
    }
  }

  public void  initiateShutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone);

  public void  awaitShutdown(): Unit = latch.await();

}

class KafkaRequestHandlerPool(val Integer brokerId,
                              val RequestChannel requestChannel,
                              val KafkaApis apis,
                              Time time,
                              Integer numThreads) extends Logging with KafkaMetricsGroup {

  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS);

  this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], ";
  val runnables = new Array<KafkaRequestHandler>(numThreads);
  for(i <- 0 until numThreads) {
    runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis, time);
    Utils.daemonThread("kafka-request-handler-" + i, runnables(i)).start();
  }

  public void  shutdown() {
    info("shutting down");
    for (handler <- runnables)
      handler.initiateShutdown();
    for (handler <- runnables)
      handler.awaitShutdown();
    info("shut down completely");
  }
}

class BrokerTopicMetrics(Option name<String>) extends KafkaMetricsGroup {
  val scala tags.collection.Map<String, String> = name match {
    case None => Map.empty;
    case Some(topic) => Map("topic" -> topic);
  }

  val messagesInRate = newMeter(BrokerTopicStats.MessagesInPerSec, "messages", TimeUnit.SECONDS, tags);
  val bytesInRate = newMeter(BrokerTopicStats.BytesInPerSec, "bytes", TimeUnit.SECONDS, tags);
  val bytesOutRate = newMeter(BrokerTopicStats.BytesOutPerSec, "bytes", TimeUnit.SECONDS, tags);
  val bytesRejectedRate = newMeter(BrokerTopicStats.BytesRejectedPerSec, "bytes", TimeUnit.SECONDS, tags);
  private<server> val replicationBytesInRate =
    if (name.isEmpty) Some(newMeter(BrokerTopicStats.ReplicationBytesInPerSec, "bytes", TimeUnit.SECONDS, tags))
    else None;
  private<server> val replicationBytesOutRate =
    if (name.isEmpty) Some(newMeter(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes", TimeUnit.SECONDS, tags))
    else None;
  val failedProduceRequestRate = newMeter(BrokerTopicStats.FailedProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags);
  val failedFetchRequestRate = newMeter(BrokerTopicStats.FailedFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags);
  val totalProduceRequestRate = newMeter(BrokerTopicStats.TotalProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags);
  val totalFetchRequestRate = newMeter(BrokerTopicStats.TotalFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags);

  public void  close() {
    removeMetric(BrokerTopicStats.MessagesInPerSec, tags);
    removeMetric(BrokerTopicStats.BytesInPerSec, tags);
    removeMetric(BrokerTopicStats.BytesOutPerSec, tags);
    removeMetric(BrokerTopicStats.BytesRejectedPerSec, tags);
    if (replicationBytesInRate.isDefined)
      removeMetric(BrokerTopicStats.ReplicationBytesInPerSec, tags);
    if (replicationBytesOutRate.isDefined)
      removeMetric(BrokerTopicStats.ReplicationBytesOutPerSec, tags);
    removeMetric(BrokerTopicStats.FailedProduceRequestsPerSec, tags);
    removeMetric(BrokerTopicStats.FailedFetchRequestsPerSec, tags);
    removeMetric(BrokerTopicStats.TotalProduceRequestsPerSec, tags);
    removeMetric(BrokerTopicStats.TotalFetchRequestsPerSec, tags);
  }
}

object BrokerTopicStats {
  val MessagesInPerSec = "MessagesInPerSec";
  val BytesInPerSec = "BytesInPerSec";
  val BytesOutPerSec = "BytesOutPerSec";
  val BytesRejectedPerSec = "BytesRejectedPerSec";
  val ReplicationBytesInPerSec = "ReplicationBytesInPerSec";
  val ReplicationBytesOutPerSec = "ReplicationBytesOutPerSec";
  val FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec";
  val FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec";
  val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec";
  val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec";
  private val valueFactory = (String k) => new BrokerTopicMetrics(Some(k));
}

class BrokerTopicStats {
  import BrokerTopicStats._;

  private val stats = new Pool<String, BrokerTopicMetrics>(Some(valueFactory));
  val allTopicsStats = new BrokerTopicMetrics(None);

  public void  topicStats(String topic): BrokerTopicMetrics =
    stats.getAndMaybePut(topic);

  public void  updateReplicationBytesIn(Long value) {
    allTopicsStats.replicationBytesInRate.foreach { metric =>
      metric.mark(value);
    }
  }

  private public void  updateReplicationBytesOut(Long value) {
    allTopicsStats.replicationBytesOutRate.foreach { metric =>
      metric.mark(value);
    }
  }

  public void  removeMetrics(String topic) {
    val metrics = stats.remove(topic);
    if (metrics != null)
      metrics.close();
  }

  public void  updateBytesOut(String topic, Boolean isFollower, Long value) {
    if (isFollower) {
      updateReplicationBytesOut(value);
    } else {
      topicStats(topic).bytesOutRate.mark(value);
      allTopicsStats.bytesOutRate.mark(value);
    }
  }


  public void  close(): Unit = {
    allTopicsStats.close();
    stats.values.foreach(_.close())
  }

}
