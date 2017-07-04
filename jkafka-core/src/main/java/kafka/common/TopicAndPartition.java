package kafka.common;

import kafka.cluster.{Partition, Replica}
import kafka.utils.Json;
import org.apache.kafka.common.TopicPartition;

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

/**
 * Convenience case class since (topic, partition) pairs are ubiquitous.
 */
case class TopicAndPartition(String topic, Integer partition) {

  public void  this(tuple: (String, Int)) = this(tuple._1, tuple._2);

  public void  this(Partition partition) = this(partition.topic, partition.partitionId);

  public void  this(TopicPartition topicPartition) = this(topicPartition.topic, topicPartition.partition);

  public void  this(Replica replica) = this(replica.topicPartition);

  public void  asTuple = (topic, partition);

  override public void  toString = String.format("<%s,%d>",topic, partition)
}
