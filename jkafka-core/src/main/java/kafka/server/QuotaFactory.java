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

import kafka.server.QuotaType._;
import kafka.utils.Logging;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;

object QuotaType  {
  case object Fetch extends QuotaType;
  case object Produce extends QuotaType;
  case object Request extends QuotaType;
  case object LeaderReplication extends QuotaType;
  case object FollowerReplication extends QuotaType;
}
sealed trait QuotaType;

object QuotaFactory extends Logging {

  object UnboundedQuota extends ReplicaQuota {
    override public void  isThrottled(TopicPartition topicPartition): Boolean = false;
    override public void  isQuotaExceeded(): Boolean = false;
  }

  case class QuotaManagers(ClientQuotaManager fetch, ClientQuotaManager produce, ClientRequestQuotaManager request, ReplicationQuotaManager leader, ReplicationQuotaManager follower) {
    public void  shutdown() {
      fetch.shutdown;
      produce.shutdown;
      request.shutdown;
    }
  }

  public void  instantiate(KafkaConfig cfg, Metrics metrics, Time time): QuotaManagers = {
    QuotaManagers(
      new ClientQuotaManager(clientFetchConfig(cfg), metrics, Fetch, time),
      new ClientQuotaManager(clientProduceConfig(cfg), metrics, Produce, time),
      new ClientRequestQuotaManager(clientRequestConfig(cfg), metrics, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, LeaderReplication, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, FollowerReplication, time);
    );
  }

  public void  clientProduceConfig(KafkaConfig cfg): ClientQuotaManagerConfig = {
    if (cfg.producerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.");
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds;
    );
  }

  public void  clientFetchConfig(KafkaConfig cfg): ClientQuotaManagerConfig = {
    if (cfg.consumerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.");
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds;
    );
  }

  public void  clientRequestConfig(KafkaConfig cfg): ClientQuotaManagerConfig = {
    ClientQuotaManagerConfig(
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds;
    );
  }

  public void  replicationConfig(KafkaConfig cfg): ReplicationQuotaManagerConfig =
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds;
    );
}
