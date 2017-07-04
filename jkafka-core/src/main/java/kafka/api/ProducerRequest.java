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

import java.nio._;

import kafka.api.ApiUtils._;
import kafka.common._;
import kafka.message._;
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response;
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

object ProducerRequest {
  val CurrentVersion = 2.shortValue;

  public void  readFrom(ByteBuffer buffer): ProducerRequest = {
    val Short versionId = buffer.getShort;
    val Integer correlationId = buffer.getInt;
    val String clientId = readShortString(buffer);
    val Short requiredAcks = buffer.getShort;
    val Integer ackTimeoutMs = buffer.getInt;
    //build the topic structure;
    val topicCount = buffer.getInt;
    val partitionDataPairs = (1 to topicCount).flatMap(_ => {
      // process topic;
      val topic = readShortString(buffer);
      val partitionCount = buffer.getInt;
      (1 to partitionCount).map(_ => {
        val partition = buffer.getInt;
        val messageSetSize = buffer.getInt;
        val messageSetBuffer = new Array<Byte>(messageSetSize);
        buffer.get(messageSetBuffer,0,messageSetSize);
        (TopicAndPartition(topic, partition), new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)));
      });
    });

    ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, collection.mutable.Map(_ partitionDataPairs*));
  }
}

case class ProducerRequest(Short versionId = ProducerRequest.CurrentVersion,
                           Integer correlationId,
                           String clientId,
                           Short requiredAcks,
                           Integer ackTimeoutMs,
                           collection data.mutable.Map<TopicAndPartition, ByteBufferMessageSet>);
    extends RequestOrResponse(Some(ApiKeys.PRODUCE.id)) {

  /**
   * Partitions the data into a map of maps (one for each topic).
   */
  private lazy val dataGroupedByTopic = data.groupBy(_._1.topic);
  val topicPartitionMessageSizeMap = data.map(r => r._1 -> r._2.sizeInBytes).toMap;

  public void  this Integer correlationId,
           String clientId,
           Short requiredAcks,
           Integer ackTimeoutMs,
           collection data.mutable.Map<TopicAndPartition, ByteBufferMessageSet>) =
    this(ProducerRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data);

  public void  writeTo(ByteBuffer buffer) {
    buffer.putShort(versionId);
    buffer.putInt(correlationId);
    writeShortString(buffer, clientId);
    buffer.putShort(requiredAcks);
    buffer.putInt(ackTimeoutMs);

    //save the topic structure;
    buffer.putInt(dataGroupedByTopic.size) //the number of topics;
    dataGroupedByTopic.foreach {
      case (topic, topicAndPartitionData) =>
        writeShortString(buffer, topic) //write the topic;
        buffer.putInt(topicAndPartitionData.size) //the number of partitions;
        topicAndPartitionData.foreach(partitionAndData => {
          val partition = partitionAndData._1.partition;
          val partitionMessageData = partitionAndData._2;
          val bytes = partitionMessageData.buffer;
          buffer.putInt(partition);
          buffer.putInt(bytes.limit);
          buffer.put(bytes);
          bytes.rewind;
        });
    }
  }

  public void  Integer sizeInBytes = {
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) + /* client id */
    2 + /* requiredAcks */
    4 + /* ackTimeoutMs */
    4 + /* number of topics */
    dataGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      foldedTopics +;
      shortStringLength(currTopic._1) +;
      4 + /* the number of partitions */
      {
        currTopic._2.foldLeft(0)((foldedPartitions, currPartition) => {
          foldedPartitions +;
          4 + /* partition id */
          4 + /* byte-length of serialized messages */
          currPartition._2.sizeInBytes;
        });
      }
    });
  }

  public void  numPartitions = data.size;

  override public void  String toString = {
    describe(true);
  }

  override public void  handleError(Throwable e, RequestChannel requestChannel, RequestChannel request.Request): Unit = {
    if (request.body<org.apache.kafka.common.requests.ProduceRequest>.acks == 0) {
        requestChannel.sendResponse(new RequestChannel.Response(request, None, RequestChannel.CloseConnectionAction));
    }
    else {
      val producerResponseStatus = data.map { case (topicAndPartition, _) =>
        (topicAndPartition, ProducerResponseStatus(Errors.forException(e), -1l, Message.NoTimestamp))
      }
      val errorResponse = ProducerResponse(correlationId, producerResponseStatus);
      requestChannel.sendResponse(Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)));
    }
  }

  override public void  describe(Boolean details): String = {
    val producerRequest = new StringBuilder;
    producerRequest.append("Name: " + this.getClass.getSimpleName);
    producerRequest.append("; Version: " + versionId);
    producerRequest.append("; CorrelationId: " + correlationId);
    producerRequest.append("; ClientId: " + clientId);
    producerRequest.append("; RequiredAcks: " + requiredAcks);
    producerRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms");
    if(details)
      producerRequest.append("; TopicAndPartition: " + topicPartitionMessageSizeMap.mkString(","));
    producerRequest.toString();
  }

  public void  emptyData(){
    data.clear();
  }
}

