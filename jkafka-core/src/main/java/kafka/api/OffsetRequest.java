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

import java.nio.ByteBuffer;

import kafka.api.ApiUtils._;
import kafka.common.TopicAndPartition;
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response;
import org.apache.kafka.common.protocol.{ApiKeys, Errors}


object OffsetRequest {
  val CurrentVersion = 0.shortValue;
  val DefaultClientId = "";

  val SmallestTimeString = "smallest";
  val LargestTimeString = "largest";
  val LatestTime = -1L;
  val EarliestTime = -2L;

  public void  readFrom(ByteBuffer buffer): OffsetRequest = {
    val versionId = buffer.getShort;
    val correlationId = buffer.getInt;
    val clientId = readShortString(buffer);
    val replicaId = buffer.getInt;
    val topicCount = buffer.getInt;
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer);
      val partitionCount = buffer.getInt;
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt;
        val time = buffer.getLong;
        val maxNumOffsets = buffer.getInt;
        (TopicAndPartition(topic, partitionId), PartitionOffsetRequestInfo(time, maxNumOffsets));
      });
    });
    OffsetRequest(Map(_ pairs*), versionId= versionId, clientId = clientId, correlationId = correlationId, replicaId = replicaId);
  }
}

case class PartitionOffsetRequestInfo(Long time, Integer maxNumOffsets);

case class OffsetRequest(Map requestInfo<TopicAndPartition, PartitionOffsetRequestInfo>,
                         Short versionId = OffsetRequest.CurrentVersion,
                         Integer correlationId = 0,
                         String clientId = OffsetRequest.DefaultClientId,
                         Integer replicaId = Request.OrdinaryConsumerId);
    extends RequestOrResponse(Some(ApiKeys.LIST_OFFSETS.id)) {

  public void  this(Map requestInfo<TopicAndPartition, PartitionOffsetRequestInfo>, Integer correlationId, Integer replicaId) = this(requestInfo, OffsetRequest.CurrentVersion, correlationId, OffsetRequest.DefaultClientId, replicaId);

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic);

  public void  writeTo(ByteBuffer buffer) {
    buffer.putShort(versionId);
    buffer.putInt(correlationId);
    writeShortString(buffer, clientId);
    buffer.putInt(replicaId);

    buffer.putInt(requestInfoGroupedByTopic.size) // topic count;
    requestInfoGroupedByTopic.foreach {
      case((topic, partitionInfos)) =>
        writeShortString(buffer, topic);
        buffer.putInt(partitionInfos.size) // partition count;
        partitionInfos.foreach {
          case (TopicAndPartition(_, partition), partitionInfo) =>
            buffer.putInt(partition);
            buffer.putLong(partitionInfo.time);
            buffer.putInt(partitionInfo.maxNumOffsets);
        }
    }
  }

  public void  sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +;
    4 + /* replicaId */
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      val (topic, partitionInfos) = currTopic;
      foldedTopics +;
      shortStringLength(topic) +;
      4 + /* partition count */
      partitionInfos.size * (
        4 + /* partition */
        8 + /* time */
        4 /* maxNumOffsets */
      );
    });

  public void  isFromOrdinaryClient = replicaId == Request.OrdinaryConsumerId;
  public void  isFromDebuggingClient = replicaId == Request.DebuggingConsumerId;

  override public void  String toString = {
    describe(true);
  }

  override  public void  handleError(Throwable e, RequestChannel requestChannel, RequestChannel request.Request): Unit = {
    val partitionOffsetResponseMap = requestInfo.map { case (topicAndPartition, _) =>
        (topicAndPartition, PartitionOffsetsResponse(Errors.forException(e), Nil))
    }
    val errorResponse = OffsetResponse(correlationId, partitionOffsetResponseMap);
    requestChannel.sendResponse(Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)));
  }

  override public void  describe(Boolean details): String = {
    val offsetRequest = new StringBuilder;
    offsetRequest.append("Name: " + this.getClass.getSimpleName);
    offsetRequest.append("; Version: " + versionId);
    offsetRequest.append("; CorrelationId: " + correlationId);
    offsetRequest.append("; ClientId: " + clientId);
    offsetRequest.append("; ReplicaId: " + replicaId);
    if(details)
      offsetRequest.append("; RequestInfo: " + requestInfo.mkString(","));
    offsetRequest.toString();
  }
}
