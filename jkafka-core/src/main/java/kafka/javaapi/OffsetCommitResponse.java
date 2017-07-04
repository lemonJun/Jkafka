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

import java.nio.ByteBuffer;

import kafka.common.TopicAndPartition;
import org.apache.kafka.common.protocol.Errors;
import scala.collection.JavaConverters._;

class OffsetCommitResponse(private val kafka underlying.api.OffsetCommitResponse) {

  public void  java errors.util.Map<TopicAndPartition, Errors> = underlying.commitStatus.asJava;

  public void  hasError = underlying.hasError;

  public void  error(TopicAndPartition topicAndPartition) = underlying.commitStatus(topicAndPartition);

  public void  errorCode(TopicAndPartition topicAndPartition) = error(topicAndPartition).code;
}

object OffsetCommitResponse {
  public void  readFrom(ByteBuffer buffer) = new OffsetCommitResponse(kafka.api.OffsetCommitResponse.readFrom(buffer));
}
