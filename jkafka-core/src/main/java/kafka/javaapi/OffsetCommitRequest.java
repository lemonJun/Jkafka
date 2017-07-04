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

import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import scala.collection.JavaConverters._;

class OffsetCommitRequest(String groupId,
                          java requestInfo.util.Map<TopicAndPartition, OffsetAndMetadata>,
                          Integer correlationId,
                          String clientId,
                          Short versionId) {
  val underlying = {
    val collection scalaMap.immutable.Map<TopicAndPartition, OffsetAndMetadata> = requestInfo.asScala.toMap;
    kafka.api.OffsetCommitRequest(
      groupId = groupId,
      requestInfo = scalaMap,
      versionId = versionId,
      correlationId = correlationId,
      clientId = clientId;
    );
  }

  public void  this(String groupId,
           java requestInfo.util.Map<TopicAndPartition, OffsetAndMetadata>,
           Integer correlationId,
           String clientId) {

    // by default bind to version 0 so that it commits to Zookeeper;
    this(groupId, requestInfo, correlationId, clientId, 0);
  }

  override public void  toString = underlying.toString;

  override public void  equals(Any obj): Boolean = {
    obj match {
      case null => false;
      case OffsetCommitRequest other => this.underlying.equals(other.underlying);
      case _ => false;
    }
  }

  override public void  hashCode = underlying.hashCode;
}
