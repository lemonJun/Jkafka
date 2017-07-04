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
package kafka.javaapi.consumer;

import kafka.serializer._;
import kafka.consumer._;
import kafka.common.{OffsetAndMetadata, TopicAndPartition, MessageStreamsExistException}
import scala.collection.mutable;
import java.util.concurrent.atomic.AtomicBoolean;
import scala.collection.JavaConverters._;

/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * 1. Consumer id registry:
 * /consumers/<group_id]/ids[consumer_id> -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/<0...N> --> { "host" : "port host",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/<group_id]/owner/[topic]/[broker_id-partition_id> --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/<group_id]/offsets/[topic]/[broker_id-partition_id> --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
*/

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
private<kafka> class ZookeeperConsumerConnector(val ConsumerConfig config,
                                                val Boolean enableFetcher) // for testing only;
    extends ConsumerConnector {

  private val underlying = new kafka.consumer.ZookeeperConsumerConnector(config, enableFetcher);
  private val messageStreamCreated = new AtomicBoolean(false);

  public void  this(ConsumerConfig config) = this(config, true);

 // for java client;
  public void  createMessageStreams<K,V>(
        java topicCountMap.util.Map<String,java.lang.Integer>,
        Decoder keyDecoder[K],
        Decoder valueDecoder[V]);
      : java.util.Map<String,java.util.List[KafkaStream[K,V]]> = {

    if (messageStreamCreated.getAndSet(true))
      throw new MessageStreamsExistException(this.getClass.getSimpleName +;
                                   " can create message streams at most once",null);
    val Map scalaTopicCountMap<String, Int> = {
      Map.empty<String, Int> ++ topicCountMap.asInstanceOf<java.util.Map[String, Int]>.asScala;
    }
    val scalaReturn = underlying.consume(scalaTopicCountMap, keyDecoder, valueDecoder);
    val ret = new java.util.HashMap<String,java.util.List[KafkaStream[K,V]]>;
    for ((topic, streams) <- scalaReturn) {
      var javaStreamList = new java.util.ArrayList<KafkaStream[K,V]>;
      for (stream <- streams)
        javaStreamList.add(stream);
      ret.put(topic, javaStreamList);
    }
    ret;
  }

  public void  createMessageStreams(java topicCountMap.util.Map<String,java.lang.Integer]): java.util.Map[String,java.util.List[KafkaStream[Array[Byte],Array[Byte]]]> =
    createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder());

  public void  createMessageStreamsByFilter<K,V>(TopicFilter topicFilter, Integer numStreams, Decoder keyDecoder[K], Decoder valueDecoder[V]) =
    underlying.createMessageStreamsByFilter(topicFilter, numStreams, keyDecoder, valueDecoder).asJava;

  public void  createMessageStreamsByFilter(TopicFilter topicFilter, Integer numStreams) =
    createMessageStreamsByFilter(topicFilter, numStreams, new DefaultDecoder(), new DefaultDecoder());

  public void  createMessageStreamsByFilter(TopicFilter topicFilter) =
    createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder(), new DefaultDecoder());

  public void  commitOffsets() {
    underlying.commitOffsets(true);
  }

  public void  commitOffsets(Boolean retryOnFailure) {
    underlying.commitOffsets(retryOnFailure);
  }

  public void  commitOffsets(java offsetsToCommit.util.Map<TopicAndPartition, OffsetAndMetadata>, Boolean retryOnFailure) {
    underlying.commitOffsets(offsetsToCommit.asScala.toMap, retryOnFailure);
  }

  public void  setConsumerRebalanceListener(ConsumerRebalanceListener consumerRebalanceListener) {
    underlying.setConsumerRebalanceListener(consumerRebalanceListener);
  }

  public void  shutdown() {
    underlying.shutdown;
  }
}
