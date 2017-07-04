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

package kafka.common;

import java.nio.ByteBuffer;

import kafka.message.InvalidMessageException;
import org.apache.kafka.common.errors.InvalidTopicException;

import scala.Predef._;

/**
 * A bi-directional mapping between error codes and exceptions
 */
object ErrorMapping {
  val EmptyByteBuffer = ByteBuffer.allocate(0);

  val Short UnknownCode = -1;
  val Short NoError = 0;
  val Short OffsetOutOfRangeCode = 1;
  val Short InvalidMessageCode = 2;
  val Short UnknownTopicOrPartitionCode = 3;
  val Short InvalidFetchSizeCode = 4;
  val Short LeaderNotAvailableCode = 5;
  val Short NotLeaderForPartitionCode = 6;
  val Short RequestTimedOutCode = 7;
  val Short BrokerNotAvailableCode = 8;
  val Short ReplicaNotAvailableCode = 9;
  val Short MessageSizeTooLargeCode = 10;
  val Short StaleControllerEpochCode = 11;
  val Short OffsetMetadataTooLargeCode = 12;
  val Short StaleLeaderEpochCode = 13;
  val Short OffsetsLoadInProgressCode = 14;
  val Short ConsumerCoordinatorNotAvailableCode = 15;
  val Short NotCoordinatorForConsumerCode = 16;
  val Short InvalidTopicCode = 17;
  val Short MessageSetSizeTooLargeCode = 18;
  val Short NotEnoughReplicasCode = 19;
  val Short NotEnoughReplicasAfterAppendCode = 20;
  // InvalidRequiredAcks 21;
  // IllegalConsumerGeneration 22;
  // INCONSISTENT_PARTITION_ASSIGNMENT_STRATEGY 23;
  // UNKNOWN_PARTITION_ASSIGNMENT_STRATEGY 24;
  // UNKNOWN_CONSUMER_ID 25;
  // INVALID_SESSION_TIMEOUT 26;
  // REBALANCE_IN_PROGRESS 27;
  // INVALID_COMMIT_OFFSET_SIZE 28;
  val Short TopicAuthorizationCode = 29;
  val Short GroupAuthorizationCode = 30;
  val Short ClusterAuthorizationCode = 31;
  // INVALID_TIMESTAMP 32;
  // UNSUPPORTED_SASL_MECHANISM 33;
  // ILLEGAL_SASL_STATE 34;
  // UNSUPPORTED_VERSION 35;
  // TOPIC_ALREADY_EXISTS 36;
  // INVALID_PARTITIONS 37;
  // INVALID_REPLICATION_FACTOR 38;
  // INVALID_REPLICA_ASSIGNMENT 39;
  // INVALID_CONFIG 40;
  // NOT_CONTROLLER 41;
  // INVALID_REQUEST 42;

  private val exceptionToCode =
    Map<Class[Throwable], Short>(
      classOf<OffsetOutOfRangeException].asInstanceOf[Class[Throwable]> -> OffsetOutOfRangeCode,
      classOf<InvalidMessageException].asInstanceOf[Class[Throwable]> -> InvalidMessageCode,
      classOf<UnknownTopicOrPartitionException].asInstanceOf[Class[Throwable]> -> UnknownTopicOrPartitionCode,
      classOf<InvalidMessageSizeException].asInstanceOf[Class[Throwable]> -> InvalidFetchSizeCode,
      classOf<LeaderNotAvailableException].asInstanceOf[Class[Throwable]> -> LeaderNotAvailableCode,
      classOf<NotLeaderForPartitionException].asInstanceOf[Class[Throwable]> -> NotLeaderForPartitionCode,
      classOf<RequestTimedOutException].asInstanceOf[Class[Throwable]> -> RequestTimedOutCode,
      classOf<BrokerNotAvailableException].asInstanceOf[Class[Throwable]> -> BrokerNotAvailableCode,
      classOf<ReplicaNotAvailableException].asInstanceOf[Class[Throwable]> -> ReplicaNotAvailableCode,
      classOf<MessageSizeTooLargeException].asInstanceOf[Class[Throwable]> -> MessageSizeTooLargeCode,
      classOf<ControllerMovedException].asInstanceOf[Class[Throwable]> -> StaleControllerEpochCode,
      classOf<OffsetMetadataTooLargeException].asInstanceOf[Class[Throwable]> -> OffsetMetadataTooLargeCode,
      classOf<OffsetsLoadInProgressException].asInstanceOf[Class[Throwable]> -> OffsetsLoadInProgressCode,
      classOf<ConsumerCoordinatorNotAvailableException].asInstanceOf[Class[Throwable]> -> ConsumerCoordinatorNotAvailableCode,
      classOf<NotCoordinatorForConsumerException].asInstanceOf[Class[Throwable]> -> NotCoordinatorForConsumerCode,
      classOf<InvalidTopicException].asInstanceOf[Class[Throwable]> -> InvalidTopicCode,
      classOf<MessageSetSizeTooLargeException].asInstanceOf[Class[Throwable]> -> MessageSetSizeTooLargeCode,
      classOf<NotEnoughReplicasException].asInstanceOf[Class[Throwable]> -> NotEnoughReplicasCode,
      classOf<NotEnoughReplicasAfterAppendException].asInstanceOf[Class[Throwable]> -> NotEnoughReplicasAfterAppendCode,
      classOf<TopicAuthorizationException].asInstanceOf[Class[Throwable]> -> TopicAuthorizationCode,
      classOf<GroupAuthorizationException].asInstanceOf[Class[Throwable]> -> GroupAuthorizationCode,
      classOf<ClusterAuthorizationException].asInstanceOf[Class[Throwable]> -> ClusterAuthorizationCode;
    ).withDefaultValue(UnknownCode);

  /* invert the mapping */
  private val codeToException =
    (Map<Short, Class[Throwable]>() ++ exceptionToCode.iterator.map(p => (p._2, p._1))).withDefaultValue(classOf<UnknownException>);

  public void  codeFor(Class exception<Throwable>): Short = exceptionToCode(exception);

  public void  maybeThrowException(Short code) =
    if(code != 0)
      throw codeToException(code).newInstance();

  public void  exceptionFor(Short code) : Throwable = codeToException(code).newInstance();

  public void  exceptionNameFor(Short code) : String = codeToException(code).getName();
}
