/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server;

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import kafka.server.Constants._;
import kafka.server.ReplicationQuotaManagerConfig._;
import kafka.utils.CoreUtils._;
import kafka.utils.Logging;
import org.apache.kafka.common.metrics._;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.stats.SimpleRate;
import org.apache.kafka.common.utils.Time;

/**
  * Configuration settings for quota management
  *
  * @param quotaBytesPerSecondDefault The default bytes per second quota allocated to internal replication
  * @param numQuotaSamples            The number of samples to retain in memory
  * @param quotaWindowSizeSeconds     The time span of each sample
  *
  */
case class ReplicationQuotaManagerConfig(Long quotaBytesPerSecondDefault = QuotaBytesPerSecondDefault,
                                         Integer numQuotaSamples = DefaultNumQuotaSamples,
                                         Integer quotaWindowSizeSeconds = DefaultQuotaWindowSizeSeconds);

object ReplicationQuotaManagerConfig {
  val QuotaBytesPerSecondDefault = Long.MaxValue;
  // Always have 10 whole windows + 1 current window;
  val DefaultNumQuotaSamples = 11;
  val DefaultQuotaWindowSizeSeconds = 1;
  // Purge sensors after 1 hour of inactivity;
  val InactiveSensorExpirationTimeSeconds = 3600;
}

trait ReplicaQuota {
  public void  isThrottled(TopicPartition topicPartition): Boolean;
  public void  isQuotaExceeded(): Boolean;
}

object Constants {
  val AllReplicas = Seq<Int>(-1);
}

/**
  * Tracks replication metrics and comparing them to any quotas for throttled partitions.
  *
  * @param config          The quota configs
  * @param metrics         The Metrics instance
  * @param replicationType The name / key for this quota manager, typically leader or follower
  * @param time            Time object to use
  */
class ReplicationQuotaManager(val ReplicationQuotaManagerConfig config,
                              private val Metrics metrics,
                              private val QuotaType replicationType,
                              private val Time time) extends Logging with ReplicaQuota {
  private val lock = new ReentrantReadWriteLock();
  private val throttledPartitions = new ConcurrentHashMap<String, Seq[Int]>();
  private var Quota quota = null;
  private val sensorAccess = new SensorAccess;
  private val rateMetricName = metrics.metricName("byte-rate", replicationType.toString, s"Tracking byte-rate for ${replicationType}")

  /**
    * Update the quota
    *
    * @param quota
    */
  public void  updateQuota(Quota quota) {
    inWriteLock(lock) {
      this.quota = quota;
      //The metric could be expired by another thread, so use a local variable and null check.;
      val metric = metrics.metrics.get(rateMetricName);
      if (metric != null) {
        metric.config(getQuotaMetricConfig(quota));
      }
    }
  }

  /**
    * Check if the quota is currently exceeded
    *
    * @return
    */
  override public void  isQuotaExceeded(): Boolean = {
    try {
      sensor().checkQuotas();
    } catch {
      case QuotaViolationException qve =>
        trace(String.format("%Quota s violated for sensor (%s), metric: (%s), metric-value: (%f), bound: (%f)",replicationType, sensor().name(), qve.metricName, qve.value, qve.bound))
        return true;
    }
    false;
  }

  /**
    * Is the passed partition throttled by this ReplicationQuotaManager
    *
    * @param topicPartition the partition to check
    * @return
    */
  override public void  isThrottled(TopicPartition topicPartition): Boolean = {
    val partitions = throttledPartitions.get(topicPartition.topic);
    if (partitions != null)
      (partitions eq AllReplicas) || partitions.contains(topicPartition.partition);
    else false;
  }

  /**
    * Add the passed value to the throttled rate. This method ignores the quota with
    * the value being added to the rate even if the quota is exceeded
    *
    * @param value
    */
  public void  record(Long value) {
    try {
      sensor().record(value);
    } catch {
      case QuotaViolationException qve =>
        trace(s"Quota Record violated, but ignored, for sensor (${sensor.name}), metric: (${qve.metricName}), value : (${qve.value}), bound: (${qve.bound}), recordedValue ($value)")
    }
  }

  /**
    * Update the set of throttled partitions for this QuotaManager. The partitions passed, for
    * any single topic, will replace any previous
    *
    * @param topic
    * @param partitions the set of throttled partitions
    * @return
    */
  public void  markThrottled(String topic, Seq partitions<Int>) {
    throttledPartitions.put(topic, partitions);
  }

  /**
    * Mark all replicas for this topic as throttled
    *
    * @param topic
    * @return
    */
  public void  markThrottled(String topic) {
    markThrottled(topic, AllReplicas);
  }

  /**
    * Remove list of throttled replicas for a certain topic
    *
    * @param topic
    * @return
    */
  public void  removeThrottle(String topic) {
    throttledPartitions.remove(topic);
  }

  /**
    * Returns the bound of the configured quota
    *
    * @return
    */
  public void  upperBound(): Long = {
    inReadLock(lock) {
      if (quota != null)
        quota.bound().toLong;
      else;
        Long.MaxValue;
    }
  }

  private public void  getQuotaMetricConfig(Quota quota): MetricConfig = {
    new MetricConfig();
      .timeWindow(config.quotaWindowSizeSeconds, TimeUnit.SECONDS);
      .samples(config.numQuotaSamples);
      .quota(quota);
  }

  private public void  sensor(): Sensor = {
    sensorAccess.getOrCreate(
      replicationType.toString,
      InactiveSensorExpirationTimeSeconds,
      lock,
      metrics,
      () => rateMetricName,
      () => getQuotaMetricConfig(quota),
      () => new SimpleRate();
    );
  }
}
