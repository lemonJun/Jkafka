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
package kafka.javaapi;

import kafka.cluster.BrokerEndPoint;
import org.apache.kafka.common.protocol.Errors;
import scala.collection.JavaConverters._;

private<javaapi> object MetadataListImplicits {
  implicit public void  toJavaTopicMetadataList(Seq topicMetadataSeq<kafka.api.TopicMetadata>):;
  java.util.List<kafka.javaapi.TopicMetadata> = topicMetadataSeq.map(new kafka.javaapi.TopicMetadata(_)).asJava;

  implicit public void  toPartitionMetadataList(Seq partitionMetadataSeq<kafka.api.PartitionMetadata>):;
  java.util.List<kafka.javaapi.PartitionMetadata> = partitionMetadataSeq.map(new kafka.javaapi.PartitionMetadata(_)).asJava;
}

class TopicMetadata(private val kafka underlying.api.TopicMetadata) {
  public void  String topic = underlying.topic;

  public void  java partitionsMetadata.util.List<PartitionMetadata> = {
    import kafka.javaapi.MetadataListImplicits._;
    underlying.partitionsMetadata;
  }

  public void  error = underlying.error;

  public void  errorCode = error.code;

  public void  Integer sizeInBytes = underlying.sizeInBytes;

  override public void  toString = underlying.toString;
}


class PartitionMetadata(private val kafka underlying.api.PartitionMetadata) {
  public void  Integer partitionId = underlying.partitionId;

  public void  BrokerEndPoint leader = {
    import kafka.javaapi.Implicits._;
    underlying.leader;
  }

  public void  java replicas.util.List<BrokerEndPoint> = underlying.replicas.asJava;

  public void  java isr.util.List<BrokerEndPoint> = underlying.isr.asJava;

  public void  error = underlying.error;

  public void  errorCode = error.code;

  public void  Integer sizeInBytes = underlying.sizeInBytes;

  override public void  toString = underlying.toString;
}
