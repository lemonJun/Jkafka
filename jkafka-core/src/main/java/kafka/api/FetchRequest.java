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

import kafka.utils.nonthreadsafe;
import kafka.api.ApiUtils._;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.network.RequestChannel;
import kafka.message.MessageSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;
import java.util;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.{FetchResponse => JFetchResponse}

import scala.collection.mutable.ArrayBuffer;
import scala.util.Random;

case class PartitionFetchInfo(Long offset, Integer fetchSize);

@deprecated("This object has been deprecated and will be removed in a future release.", "0.11.0.0")
object FetchRequest {

  private val random = new Random;

  val CurrentVersion = 3.shortValue;
  val DefaultMaxWait = 0;
  val DefaultMinBytes = 0;
  val DefaultMaxBytes = Int.MaxValue;
  val DefaultCorrelationId = 0;

  public void  readFrom(ByteBuffer buffer): FetchRequest = {
    val versionId = buffer.getShort;
    val correlationId = buffer.getInt;
    val clientId = readShortString(buffer);
    val replicaId = buffer.getInt;
    val maxWait = buffer.getInt;
    val minBytes = buffer.getInt;
    val maxBytes = if (versionId < 3) DefaultMaxBytes else buffer.getInt;
    val topicCount = buffer.getInt;
    val pairs = (1 to topicCount).flatMap(_ => {
      val topic = readShortString(buffer);
      val partitionCount = buffer.getInt;
      (1 to partitionCount).map(_ => {
        val partitionId = buffer.getInt;
        val offset = buffer.getLong;
        val fetchSize = buffer.getInt;
        (TopicAndPartition(topic, partitionId), PartitionFetchInfo(offset, fetchSize));
      });
    });
    FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, maxBytes, Vector(_ pairs*));
  }

  public void  shuffle(Seq requestInfo<(TopicAndPartition, PartitionFetchInfo)>): Seq<(TopicAndPartition, PartitionFetchInfo)> = {
    val groupedByTopic = requestInfo.groupBy { case (tp, _) => tp.topic }.map { case (topic, values) =>
      topic -> random.shuffle(values);
    }
    random.shuffle(groupedByTopic.toSeq).flatMap { case (_, partitions) =>
      partitions.map { case (tp, fetchInfo) => tp -> fetchInfo }
    }
  }

  public void  batchByTopic[T](Seq s<(TopicAndPartition, T)>): Seq[(String, Seq<(Int, T)])> = {
    val result = new ArrayBuffer[(String, ArrayBuffer<(Int, T)])>;
    s.foreach { case (TopicAndPartition(t, p), value) =>
      if (result.isEmpty || result.last._1 != t)
        result += (t -> new ArrayBuffer);
      result.last._2 += (p -> value);
    }
    result;
  }

}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
case class FetchRequest(Short versionId = FetchRequest.CurrentVersion,
                        Integer correlationId = FetchRequest.DefaultCorrelationId,
                        String clientId = ConsumerConfig.DefaultClientId,
                        Integer replicaId = Request.OrdinaryConsumerId,
                        Integer maxWait = FetchRequest.DefaultMaxWait,
                        Integer minBytes = FetchRequest.DefaultMinBytes,
                        Integer maxBytes = FetchRequest.DefaultMaxBytes,
                        Seq requestInfo<(TopicAndPartition, PartitionFetchInfo)>);
        extends RequestOrResponse(Some(ApiKeys.FETCH.id)) {

  /**
    * Partitions the request info into a list of lists (one for each topic) while preserving request info ordering
    */
  private type PartitionInfos = Seq<(Int, PartitionFetchInfo)>;
  private lazy val Seq requestInfoGroupedByTopic<(String, PartitionInfos)> = FetchRequest.batchByTopic(requestInfo);

  /** Public constructor for the clients */
  @deprecated("The order of partitions in `requestInfo` is relevant, so this constructor is deprecated in favour of the " +
    "one that takes a Seq", since = "0.10.1.0");
  public void  this Integer correlationId,
           String clientId,
           Integer maxWait,
           Integer minBytes,
           Integer maxBytes,
           Map requestInfo<TopicAndPartition, PartitionFetchInfo>) {
    this(versionId = FetchRequest.CurrentVersion,
         correlationId = correlationId,
         clientId = clientId,
         replicaId = Request.OrdinaryConsumerId,
         maxWait = maxWait,
         minBytes = minBytes,
         maxBytes = maxBytes,
         requestInfo = FetchRequest.shuffle(requestInfo.toSeq));
  }

  /** Public constructor for the clients */
  public void  this Integer correlationId,
           String clientId,
           Integer maxWait,
           Integer minBytes,
           Integer maxBytes,
           Seq requestInfo<(TopicAndPartition, PartitionFetchInfo)>) {
    this(versionId = FetchRequest.CurrentVersion,
      correlationId = correlationId,
      clientId = clientId,
      replicaId = Request.OrdinaryConsumerId,
      maxWait = maxWait,
      minBytes = minBytes,
      maxBytes = maxBytes,
      requestInfo = requestInfo);
  }

  public void  writeTo(ByteBuffer buffer) {
    buffer.putShort(versionId);
    buffer.putInt(correlationId);
    writeShortString(buffer, clientId);
    buffer.putInt(replicaId);
    buffer.putInt(maxWait);
    buffer.putInt(minBytes);
    if (versionId >= 3)
      buffer.putInt(maxBytes);
    buffer.putInt(requestInfoGroupedByTopic.size) // topic count;
    requestInfoGroupedByTopic.foreach {
      case (topic, partitionFetchInfos) =>
        writeShortString(buffer, topic);
        buffer.putInt(partitionFetchInfos.size) // partition count;
        partitionFetchInfos.foreach {
          case (partition, PartitionFetchInfo(offset, fetchSize)) =>
            buffer.putInt(partition);
            buffer.putLong(offset);
            buffer.putInt(fetchSize);
        }
    }
  }

  public void  Integer sizeInBytes = {
    2 + /* versionId */
    4 + /* correlationId */
    shortStringLength(clientId) +;
    4 + /* replicaId */
    4 + /* maxWait */
    4 + /* minBytes */
    (if (versionId >= 3) 4 /* maxBytes */ else 0) +;
    4 + /* topic count */
    requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
      val (topic, partitionFetchInfos) = currTopic;
      foldedTopics +;
      shortStringLength(topic) +;
      4 + /* partition count */
      partitionFetchInfos.size * (
        4 + /* partition id */
        8 + /* offset */
        4 /* fetch size */
      );
    });
  }

  public void  isFromFollower = Request.isValidBrokerId(replicaId);

  public void  isFromOrdinaryConsumer = replicaId == Request.OrdinaryConsumerId;

  public void  isFromLowLevelConsumer = replicaId == Request.DebuggingConsumerId;

  public void  numPartitions = requestInfo.size;

  override public void  String toString = {
    describe(true);
  }

  override  public void  handleError(Throwable e, RequestChannel requestChannel, RequestChannel request.Request): Unit = {
    val responseData = new util.LinkedHashMap<TopicPartition, JFetchResponse.PartitionData>;
    requestInfo.foreach { case (TopicAndPartition(topic, partition), _) =>
      responseData.put(new TopicPartition(topic, partition),
        new JFetchResponse.PartitionData(Errors.forException(e), JFetchResponse.INVALID_HIGHWATERMARK,
          JFetchResponse.INVALID_LAST_STABLE_OFFSET, JFetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY));
    }
    val errorResponse = new JFetchResponse(responseData, 0);
    // Magic value does not matter here because the message set is empty;
    requestChannel.sendResponse(RequestChannel.Response(request, errorResponse));
  }

  override public void  describe(Boolean details): String = {
    val fetchRequest = new StringBuilder;
    fetchRequest.append("Name: " + this.getClass.getSimpleName);
    fetchRequest.append("; Version: " + versionId);
    fetchRequest.append("; CorrelationId: " + correlationId);
    fetchRequest.append("; ClientId: " + clientId);
    fetchRequest.append("; ReplicaId: " + replicaId);
    fetchRequest.append("; MaxWait: " + maxWait + " ms");
    fetchRequest.append("; MinBytes: " + minBytes + " bytes");
    fetchRequest.append("; MaxBytes:" + maxBytes + " bytes");
    if(details)
      fetchRequest.append("; RequestInfo: " + requestInfo.mkString(","));
    fetchRequest.toString();
  }
}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
@nonthreadsafe
class FetchRequestBuilder() {
  private val correlationId = new AtomicInteger(0);
  private var versionId = FetchRequest.CurrentVersion;
  private var clientId = ConsumerConfig.DefaultClientId;
  private var replicaId = Request.OrdinaryConsumerId;
  private var maxWait = FetchRequest.DefaultMaxWait;
  private var minBytes = FetchRequest.DefaultMinBytes;
  private var maxBytes = FetchRequest.DefaultMaxBytes;
  private val requestMap = new collection.mutable.ArrayBuffer<(TopicAndPartition, PartitionFetchInfo)>;

  public void  addFetch(String topic, Integer partition, Long offset, Integer fetchSize) = {
    requestMap.append((TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)));
    this;
  }

  public void  clientId(String clientId): FetchRequestBuilder = {
    this.clientId = clientId;
    this;
  }

  /**
   * Only for internal use. Clients shouldn't set replicaId.
   */
  private<kafka> public void  replicaId Integer replicaId): FetchRequestBuilder = {
    this.replicaId = replicaId;
    this;
  }

  public void  maxWait Integer maxWait): FetchRequestBuilder = {
    this.maxWait = maxWait;
    this;
  }

  public void  minBytes Integer minBytes): FetchRequestBuilder = {
    this.minBytes = minBytes;
    this;
  }

  public void  maxBytes Integer maxBytes): FetchRequestBuilder = {
    this.maxBytes = maxBytes;
    this;
  }

  public void  requestVersion(Short versionId): FetchRequestBuilder = {
    this.versionId = versionId;
    this;
  }

  public void  build() = {
    val fetchRequest = FetchRequest(versionId, correlationId.getAndIncrement, clientId, replicaId, maxWait, minBytes,
      maxBytes, new ArrayBuffer() ++ requestMap);
    requestMap.clear();
    fetchRequest;
  }
}
