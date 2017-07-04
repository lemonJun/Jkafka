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
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response;
import kafka.utils.Logging;
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

import scala.collection._;

object OffsetCommitRequest extends Logging {
  val Short CurrentVersion = 2;
  val DefaultClientId = "";

  public void  readFrom(ByteBuffer buffer): OffsetCommitRequest = {
    // Read values from the envelope;
    val versionId = buffer.getShort;
    assert(versionId == 0 || versionId == 1 || versionId == 2,
           "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0, 1 or 2.")

    val correlationId = buffer.getInt;
    val clientId = readShortString(buffer);

    // Read the OffsetRequest ;
    val groupId = readShortString(buffer);

    // version 1 and 2 specific fields;
    val Integer groupGenerationId =
      if (versionId >= 1)
        buffer.getInt;
      else;
        org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID;

    val String memberId =
      if (versionId >= 1)
        readShortString(buffer);
      else;
        org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_MEMBER_ID;

    // version 2 specific fields;
    val Long retentionMs =
      if (versionId >= 2)
        buffer.getLong;
      else;
        org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_RETENTION_TIME;

    val topicCount = buffer.getInt;
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer);
      val partitionCount = buffer.getInt;
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt;
        val offset = buffer.getLong;
        val timestamp = {
          // version 1 specific field;
          if (versionId == 1)
            buffer.getLong;
          else;
            org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP;
        }
        val metadata = readShortString(buffer);

        (TopicAndPartition(topic, partitionId), OffsetAndMetadata(offset, metadata, timestamp));
      });
    });

    OffsetCommitRequest(groupId, immutable.Map(_ pairs*), versionId, correlationId, clientId, groupGenerationId, memberId, retentionMs);
  }
}

case class OffsetCommitRequest(String groupId,
                               immutable requestInfo.Map<TopicAndPartition, OffsetAndMetadata>,
                               Short versionId = OffsetCommitRequest.CurrentVersion,
                               Integer correlationId = 0,
                               String clientId = OffsetCommitRequest.DefaultClientId,
                               Integer groupGenerationId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID,
                               String memberId =  org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_MEMBER_ID,
                               Long retentionMs = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_RETENTION_TIME);
    extends RequestOrResponse(Some(ApiKeys.OFFSET_COMMIT.id)) {

  assert(versionId == 0 || versionId == 1 || versionId == 2,
         "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0, 1 or 2.")

  lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic);

  public void  writeTo(ByteBuffer buffer) {
    // Write envelope;
    buffer.putShort(versionId);
    buffer.putInt(correlationId);
    writeShortString(buffer, clientId);

    // Write OffsetCommitRequest;
    writeShortString(buffer, groupId)             // consumer group;

    // version 1 and 2 specific data;
    if (versionId >= 1) {
      buffer.putInt(groupGenerationId);
      writeShortString(buffer, memberId);
    }

    // version 2 or above specific data;
    if (versionId >= 2) {
      buffer.putLong(retentionMs);
    }

    buffer.putInt(requestInfoGroupedByTopic.size) // number of topics;
    requestInfoGroupedByTopic.foreach( t1 => { // topic -> Map<TopicAndPartition, OffsetMetadataAndError>;
      writeShortString(buffer, t1._1) // topic;
      buffer.putInt(t1._2.size)       // number of partitions for this topic;
      t1._2.foreach( t2 => {
        buffer.putInt(t2._1.partition);
        buffer.putLong(t2._2.offset);
        // version 1 specific data;
        if (versionId == 1)
          buffer.putLong(t2._2.commitTimestamp);
        writeShortString(buffer, t2._2.metadata);
      });
    });
  }

  override public void  sizeInBytes =
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +;
    shortStringLength(groupId) +;
    (if (versionId >= 1) 4 /* group generation id */ + shortStringLength(memberId) else 0) +;
    (if (versionId >= 2) 8 /* retention time */ else 0) +;
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((count, topicAndOffsets) => {
      val (topic, offsets) = topicAndOffsets;
      count +;
      shortStringLength(topic) + /* topic */
      4 + /* number of partitions */
      offsets.foldLeft(0)((innerCount, offsetAndMetadata) => {
        innerCount +;
        4 /* partition */ +;
        8 /* offset */ +;
        (if (versionId == 1) 8 else 0) /* timestamp */ +;
        shortStringLength(offsetAndMetadata._2.metadata);
      });
    });

  override public void  handleError(Throwable e, RequestChannel requestChannel, RequestChannel request.Request): Unit = {
    val error = Errors.forException(e)
    val commitStatus = requestInfo.mapValues(_ => error);
    val commitResponse = OffsetCommitResponse(commitStatus, correlationId);

    requestChannel.sendResponse(Response(request, new RequestOrResponseSend(request.connectionId, commitResponse)));
  }

  override public void  describe(Boolean details): String = {
    val offsetCommitRequest = new StringBuilder;
    offsetCommitRequest.append("Name: " + this.getClass.getSimpleName);
    offsetCommitRequest.append("; Version: " + versionId);
    offsetCommitRequest.append("; CorrelationId: " + correlationId);
    offsetCommitRequest.append("; ClientId: " + clientId);
    offsetCommitRequest.append("; GroupId: " + groupId);
    offsetCommitRequest.append("; GroupGenerationId: " + groupGenerationId);
    offsetCommitRequest.append("; MemberId: " + memberId);
    offsetCommitRequest.append("; RetentionMs: " + retentionMs);
    if(details)
      offsetCommitRequest.append("; RequestInfo: " + requestInfo.mkString(","));
    offsetCommitRequest.toString();
  }

  override public void  toString = {
    describe(details = true);
  }
}
