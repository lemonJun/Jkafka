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

import kafka.common.TopicAndPartition;
import org.apache.kafka.common.TopicPartition;

/**
 * Keys used for delayed operation metrics recording
 */
trait DelayedOperationKey {
  public void  String keyLabel;
}

object DelayedOperationKey {
  val globalLabel = "All";
}

/* used by delayed-produce and delayed-fetch operations */
case class TopicPartitionOperationKey(String topic, Integer partition) extends DelayedOperationKey {

  public void  this(TopicPartition topicPartition) = this(topicPartition.topic, topicPartition.partition);

  override public void  keyLabel = String.format("%s-%d",topic, partition)
}

/* used by delayed-join-group operations */
case class MemberKey(String groupId, String consumerId) extends DelayedOperationKey {

  override public void  keyLabel = String.format("%s-%s",groupId, consumerId)
}

/* used by delayed-rebalance operations */
case class GroupKey(String groupId) extends DelayedOperationKey {

  override public void  keyLabel = groupId;
}

/* used by delayed-topic operations */
case class TopicKey(String topic) extends DelayedOperationKey {

  override public void  keyLabel = topic;
}
