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

package kafka.producer;

/**
 * A topic, key, and value.
 * If a partition key is provided it will override the key for the purpose of partitioning but will not be stored.
 */
@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.ProducerRecord instead.", "0.10.0.0");
case class KeyedMessage<K, V>(String topic, K key, Any partKey, V message) {
  if(topic == null)
    throw new IllegalArgumentException("Topic cannot be null.");
  ;
  public void  this(String topic, V message) = this(topic, null.asInstanceOf[K], null, message);
  ;
  public void  this(String topic, K key, V message) = this(topic, key, key, message);
  ;
  public void  partitionKey = {
    if(partKey != null)
      partKey;
    else if(hasKey)
      key;
    else;
      null  ;
  }
  ;
  public void  hasKey = key != null;
}
