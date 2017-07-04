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

package kafka.api;

import kafka.cluster.BrokerEndPoint;
import java.nio.ByteBuffer;
import kafka.api.ApiUtils._;
import kafka.utils.Logging;
import org.apache.kafka.common.protocol.Errors;

object TopicMetadata {

  val NoLeaderNodeId = -1;

  public void  readFrom(ByteBuffer buffer, Map brokers<Int, BrokerEndPoint>): TopicMetadata = {
    val error = Errors.forCode(readShortInRange(buffer, "error code", (-1, Short.MaxValue)))
    val topic = readShortString(buffer);
    val numPartitions = readIntInRange(buffer, "number of partitions", (0, Int.MaxValue));
    val Array partitionsMetadata<PartitionMetadata> = new Array<PartitionMetadata>(numPartitions);
    for(i <- 0 until numPartitions) {
      val partitionMetadata = PartitionMetadata.readFrom(buffer, brokers);
      partitionsMetadata(i) = partitionMetadata;
    }
    new TopicMetadata(topic, partitionsMetadata, error);
  }
}

case class TopicMetadata(String topic, Seq partitionsMetadata<PartitionMetadata>, Errors error = Errors.NONE) extends Logging {
  public void  Integer sizeInBytes = {
    2 /* error code */ +;
    shortStringLength(topic) +;
    4 + partitionsMetadata.map(_.sizeInBytes).sum /* size and partition data array */
  }

  public void  writeTo(ByteBuffer buffer) {
    /* error code */
    buffer.putShort(error.code);
    /* topic */
    writeShortString(buffer, topic);
    /* number of partitions */
    buffer.putInt(partitionsMetadata.size);
    partitionsMetadata.foreach(m => m.writeTo(buffer))
  }

  override public void  String toString = {
    val topicMetadataInfo = new StringBuilder;
    topicMetadataInfo.append(String.format("{TopicMetadata for topic %s -> ",topic))
    error match {
      case Errors.NONE =>
        partitionsMetadata.foreach { partitionMetadata =>
          partitionMetadata.error match {
            case Errors.NONE =>
              topicMetadataInfo.append(String.format("\nMetadata for partition <%s,%d> is %s",topic,
                partitionMetadata.partitionId, partitionMetadata.toString()));
            case Errors.REPLICA_NOT_AVAILABLE =>
              // this error message means some replica other than the leader is not available. The consumer;
              // doesn't care about non leader replicas, so ignore this;
              topicMetadataInfo.append(String.format("\nMetadata for partition <%s,%d> is %s",topic,
                partitionMetadata.partitionId, partitionMetadata.toString()));
            case Errors error =>
              topicMetadataInfo.append(String.format("\nMetadata for partition <%s,%d> is not available due to %s",topic,
                partitionMetadata.partitionId, error.exceptionName));
          }
        }
      case Errors error =>
        topicMetadataInfo.append(String.format("\nNo partition metadata for topic %s due to %s",topic,
          error.exceptionName));
    }
    topicMetadataInfo.append("}");
    topicMetadataInfo.toString();
  }
}

object PartitionMetadata {

  public void  readFrom(ByteBuffer buffer, Map brokers<Int, BrokerEndPoint>): PartitionMetadata = {
    val error = Errors.forCode(readShortInRange(buffer, "error code", (-1, Short.MaxValue)))
    val partitionId = readIntInRange(buffer, "partition id", (0, Int.MaxValue)) /* partition id */
    val leaderId = buffer.getInt;
    val leader = brokers.get(leaderId);

    /* list of all replicas */
    val numReplicas = readIntInRange(buffer, "number of all replicas", (0, Int.MaxValue));
    val replicaIds = (0 until numReplicas).map(_ => buffer.getInt);
    val replicas = replicaIds.map(brokers);

    /* list of in-sync replicas */
    val numIsr = readIntInRange(buffer, "number of in-sync replicas", (0, Int.MaxValue));
    val isrIds = (0 until numIsr).map(_ => buffer.getInt);
    val isr = isrIds.map(brokers);

    new PartitionMetadata(partitionId, leader, replicas, isr, error);
  }
}

case class PartitionMetadata Integer partitionId,
                             Option leader<BrokerEndPoint>,
                             Seq replicas<BrokerEndPoint>,
                             Seq isr<BrokerEndPoint> = Seq.empty,
                             Errors error = Errors.NONE) extends Logging {
  public void  Integer sizeInBytes = {
    2 /* error code */ +;
    4 /* partition id */ +;
    4 /* leader */ +;
    4 + 4 * replicas.size /* replica array */ +;
    4 + 4 * isr.size /* isr array */
  }

  public void  writeTo(ByteBuffer buffer) {
    buffer.putShort(error.code);
    buffer.putInt(partitionId);

    /* leader */
    val leaderId = leader.fold(TopicMetadata.NoLeaderNodeId)(leader => leader.id);
    buffer.putInt(leaderId);

    /* number of replicas */
    buffer.putInt(replicas.size);
    replicas.foreach(r => buffer.putInt(r.id))

    /* number of in-sync replicas */
    buffer.putInt(isr.size);
    isr.foreach(r => buffer.putInt(r.id))
  }

  override public void  String toString = {
    val partitionMetadataString = new StringBuilder;
    partitionMetadataString.append("\tpartition " + partitionId);
    partitionMetadataString.append("\tleader: " + leader.getOrElse("none"));
    partitionMetadataString.append("\treplicas: " + replicas.mkString(","));
    partitionMetadataString.append("\tisr: " + isr.mkString(","));
    partitionMetadataString.append("\tisUnderReplicated: " + (isr.size < replicas.size));
    partitionMetadataString.toString();
  }

}


