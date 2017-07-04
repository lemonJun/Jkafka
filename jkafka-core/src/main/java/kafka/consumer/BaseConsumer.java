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

package kafka.consumer;

import java.util.{Collections, Properties}
import java.util.regex.Pattern;

import kafka.api.OffsetRequest;
import kafka.common.StreamEndException;
import kafka.message.Message;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * A base consumer used to abstract both old and new consumer
 * this class should be removed (along with BaseProducer)
 * once we deprecate old consumer
 */
@deprecated("This trait has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.KafkaConsumer instead.", "0.11.0.0");
trait BaseConsumer {
  public void  receive(): BaseConsumerRecord;
  public void  stop();
  public void  cleanup();
  public void  commit();
}

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.ConsumerRecord instead.", "0.11.0.0");
case class BaseConsumerRecord(String topic,
                              Integer partition,
                              Long offset,
                              Long timestamp = Message.NoTimestamp,
                              TimestampType timestampType = TimestampType.NO_TIMESTAMP_TYPE,
                              Array key<Byte>,
                              Array value<Byte>,
                              Headers headers = new RecordHeaders());

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.KafkaConsumer instead.", "0.11.0.0");
class NewShinyConsumer(Option topic<String>, Option partitionId<Int>, Option offset<Long>, Option whitelist<String>, Properties consumerProps, val Long timeoutMs = Long.MaxValue) extends BaseConsumer {
  import org.apache.kafka.clients.consumer.KafkaConsumer;

  val consumer = new KafkaConsumer<Array[Byte], Array[Byte]>(consumerProps);
  consumerInit();
  var recordIter = consumer.poll(0).iterator;

  public void  consumerInit() {
    (topic, partitionId, offset, whitelist) match {
      case (Some(topic), Some(partitionId), Some(offset), None) =>
        seek(topic, partitionId, offset);
      case (Some(topic), Some(partitionId), None, None) =>
        // default to latest if no offset is provided;
        seek(topic, partitionId, OffsetRequest.LatestTime);
      case (Some(topic), None, None, None) =>
        consumer.subscribe(Collections.singletonList(topic));
      case (None, None, None, Some(whitelist)) =>
        consumer.subscribe(Pattern.compile(whitelist), new NoOpConsumerRebalanceListener());
      case _ =>
        throw new IllegalArgumentException("An invalid combination of arguments is provided. " +;
            "Exactly one of 'topic' or 'whitelist' must be provided. " +;
            "If 'topic' is provided, an optional 'partition' may also be provided. " +;
            "If 'partition' is provided, an optional 'offset' may also be provided, otherwise, consumption starts from the end of the partition.");
    }
  }

  public void  seek(String topic, Integer partitionId, Long offset) {
    val topicPartition = new TopicPartition(topic, partitionId);
    consumer.assign(Collections.singletonList(topicPartition));
    offset match {
      case OffsetRequest.EarliestTime => consumer.seekToBeginning(Collections.singletonList(topicPartition));
      case OffsetRequest.LatestTime => consumer.seekToEnd(Collections.singletonList(topicPartition));
      case _ => consumer.seek(topicPartition, offset);
    }
  }

  override public void  receive(): BaseConsumerRecord = {
    if (!recordIter.hasNext) {
      recordIter = consumer.poll(timeoutMs).iterator;
      if (!recordIter.hasNext)
        throw new ConsumerTimeoutException;
    }

    val record = recordIter.next;
    BaseConsumerRecord(record.topic,
                       record.partition,
                       record.offset,
                       record.timestamp,
                       record.timestampType,
                       record.key,
                       record.value,
                       record.headers);
  }

  override public void  stop() {
    this.consumer.wakeup();
  }

  override public void  cleanup() {
    this.consumer.close();
  }

  override public void  commit() {
    this.consumer.commitSync();
  }
}

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.KafkaConsumer instead.", "0.11.0.0");
class OldConsumer(TopicFilter topicFilter, Properties consumerProps) extends BaseConsumer {
  import kafka.serializer.DefaultDecoder;

  val consumerConnector = Consumer.create(new ConsumerConfig(consumerProps));
  val KafkaStream stream<Array[Byte], Array[Byte]> =
    consumerConnector.createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder(), new DefaultDecoder()).head;
  val iter = stream.iterator;

  override public void  receive(): BaseConsumerRecord = {
    if (!iter.hasNext())
      throw new StreamEndException;

    val messageAndMetadata = iter.next;
    BaseConsumerRecord(messageAndMetadata.topic,
                       messageAndMetadata.partition,
                       messageAndMetadata.offset,
                       messageAndMetadata.timestamp,
                       messageAndMetadata.timestampType,
                       messageAndMetadata.key,
                       messageAndMetadata.message, ;
                       new RecordHeaders());
  }

  override public void  stop() {
    this.consumerConnector.shutdown();
  }

  override public void  cleanup() {
    this.consumerConnector.shutdown();
  }

  override public void  commit() {
    this.consumerConnector.commitOffsets;
  }
}
