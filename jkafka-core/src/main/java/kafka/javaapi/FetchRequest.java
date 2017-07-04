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

import java.util;

import kafka.common.TopicAndPartition;
import kafka.api.{PartitionFetchInfo, Request}

import scala.collection.JavaConverters._;

object FetchRequest {
  private public void  seqToLinkedHashMap<K, V>(Seq s<(K, V)>): util.LinkedHashMap<K, V> = {
    val map = new util.LinkedHashMap<K, V>;
    s.foreach { case (k, v) => map.put(k, v) }
    map;
  }
}

class FetchRequest Integer correlationId,
                   String clientId,
                   Integer maxWait,
                   Integer minBytes,
                   util requestInfo.LinkedHashMap<TopicAndPartition, PartitionFetchInfo>) {

  @deprecated("The order of partitions in `requestInfo` is relevant, so this constructor is deprecated in favour of the " +
    "one that takes a LinkedHashMap", since = "0.10.1.0");
  public void  this Integer correlationId, String clientId, Integer maxWait, Integer minBytes,
    java requestInfo.util.Map<TopicAndPartition, PartitionFetchInfo>) {
    this(correlationId, clientId, maxWait, minBytes,
      FetchRequest.seqToLinkedHashMap(kafka.api.FetchRequest.shuffle(requestInfo.asScala.toSeq)));
  }

  val underlying = kafka.api.FetchRequest(
    correlationId = correlationId,
    clientId = clientId,
    replicaId = Request.OrdinaryConsumerId,
    maxWait = maxWait,
    minBytes = minBytes,
    requestInfo = requestInfo.asScala.toBuffer;
  );

  override public void  toString = underlying.toString;

  override public void  equals(Any obj): Boolean = {
    obj match {
      case null => false;
      case FetchRequest other => this.underlying.equals(other.underlying);
      case _ => false;
    }
  }

  override public void  hashCode = underlying.hashCode;
}

