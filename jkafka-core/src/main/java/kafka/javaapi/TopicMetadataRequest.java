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

import kafka.api._;
import org.apache.kafka.common.protocol.ApiKeys;

import scala.collection.JavaConverters._;

class TopicMetadataRequest(val Short versionId,
                           val Integer correlationId,
                           val String clientId,
                           val java topics.util.List<String>);
    extends RequestOrResponse(Some(ApiKeys.METADATA.id)) {

  val kafka underlying.api.TopicMetadataRequest = new kafka.api.TopicMetadataRequest(versionId, correlationId, clientId, topics.asScala);

  public void  this(java topics.util.List<String>) =
    this(kafka.api.TopicMetadataRequest.CurrentVersion, 0, kafka.api.TopicMetadataRequest.DefaultClientId, topics);

  public void  this(java topics.util.List<String>, Integer correlationId) =
    this(kafka.api.TopicMetadataRequest.CurrentVersion, correlationId, kafka.api.TopicMetadataRequest.DefaultClientId, topics);

  public void  writeTo(ByteBuffer buffer) = underlying.writeTo(buffer);

  public void  Integer sizeInBytes = underlying.sizeInBytes();

  override public void  String toString = {
    describe(true);
  }

  override public void  describe(Boolean details): String = {
    val topicMetadataRequest = new StringBuilder;
    topicMetadataRequest.append("Name: " + this.getClass.getSimpleName);
    topicMetadataRequest.append("; Version: " + versionId);
    topicMetadataRequest.append("; CorrelationId: " + correlationId);
    topicMetadataRequest.append("; ClientId: " + clientId);
    if(details) {
      topicMetadataRequest.append("; Topics: ");
      val topicIterator = topics.iterator();
      while (topicIterator.hasNext) {
        val topic = topicIterator.next();
        topicMetadataRequest.append(String.format("%s",topic))
        if(topicIterator.hasNext)
          topicMetadataRequest.append(",");
      }
    }
    topicMetadataRequest.toString();
  }
}
