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

package kafka.consumer;

import scala.collection._;
import kafka.utils.{Json, ZKGroupDirs, ZkUtils, Logging, CoreUtils}
import kafka.common.KafkaException;

@deprecated("This trait has been deprecated and will be removed in a future release.", "0.11.0.0")
private<kafka> trait TopicCount {

  public void  Map getConsumerThreadIdsPerTopic<String, Set[ConsumerThreadId]>;
  public void  Map getTopicCountMap<String, Int>;
  public void  String pattern;

}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
case class ConsumerThreadId(String consumer, Integer threadId) extends Ordered<ConsumerThreadId> {
  override public void  toString = String.format("%s-%d",consumer, threadId)

  public void  compare(ConsumerThreadId that) = toString.compare(that.toString);
}

@deprecated("This object has been deprecated and will be removed in a future release.", "0.11.0.0")
private<kafka> object TopicCount extends Logging {
  val whiteListPattern = "white_list";
  val blackListPattern = "black_list";
  val staticPattern = "static";

  public void  makeThreadId(String consumerIdString, Integer threadId) = consumerIdString + "-" + threadId;

  public void  makeConsumerThreadIdsPerTopic(String consumerIdString,
                                    Map topicCountMap[String,  Int]) = {
    val consumerThreadIdsPerTopicMap = new mutable.HashMap<String, Set[ConsumerThreadId]>();
    for ((topic, nConsumers) <- topicCountMap) {
      val consumerSet = new mutable.HashSet<ConsumerThreadId>;
      assert(nConsumers >= 1);
      for (i <- 0 until nConsumers)
        consumerSet += ConsumerThreadId(consumerIdString, i);
      consumerThreadIdsPerTopicMap.put(topic, consumerSet);
    }
    consumerThreadIdsPerTopicMap;
  }

  public void  constructTopicCount(String group, String consumerId, ZkUtils zkUtils, Boolean excludeInternalTopics) : TopicCount = {
    val dirs = new ZKGroupDirs(group);
    val topicCountString = zkUtils.readData(dirs.consumerRegistryDir + "/" + consumerId)._1;
    var String subscriptionPattern = null;
    var Map topMap<String, Int> = null;
    try {
      Json.parseFull(topicCountString) match {
        case Some(m) =>
          val consumerRegistrationMap = m.asInstanceOf<Map[String, Any]>;
          consumerRegistrationMap.get("pattern") match {
            case Some(pattern) => subscriptionPattern = pattern.asInstanceOf<String>;
            case None => throw new KafkaException("error constructing TopicCount : " + topicCountString);
          }
          consumerRegistrationMap.get("subscription") match {
            case Some(sub) => topMap = sub.asInstanceOf<Map[String, Int]>;
            case None => throw new KafkaException("error constructing TopicCount : " + topicCountString);
          }
        case None => throw new KafkaException("error constructing TopicCount : " + topicCountString);
      }
    } catch {
      case Throwable e =>
        error("error parsing consumer json string " + topicCountString, e);
        throw e;
    }

    val hasWhiteList = whiteListPattern.equals(subscriptionPattern);
    val hasBlackList = blackListPattern.equals(subscriptionPattern);

    if (topMap.isEmpty || !(hasWhiteList || hasBlackList)) {
      new StaticTopicCount(consumerId, topMap);
    } else {
      val regex = topMap.head._1;
      val numStreams = topMap.head._2;
      val filter =
        if (hasWhiteList)
          new Whitelist(regex);
        else;
          new Blacklist(regex);
      new WildcardTopicCount(zkUtils, consumerId, filter, numStreams, excludeInternalTopics);
    }
  }

  public void  constructTopicCount(String consumerIdString, Map topicCount<String, Int>) =
    new StaticTopicCount(consumerIdString, topicCount);

  public void  constructTopicCount(String consumerIdString, TopicFilter filter, Integer numStreams, ZkUtils zkUtils, Boolean excludeInternalTopics) =
    new WildcardTopicCount(zkUtils, consumerIdString, filter, numStreams, excludeInternalTopics);

}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
private<kafka> class StaticTopicCount(val String consumerIdString,
                                val Map topicCountMap<String, Int>);
                                extends TopicCount {

  public void  getConsumerThreadIdsPerTopic = TopicCount.makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap);

  public void  getTopicCountMap = topicCountMap;

  public void  pattern = TopicCount.staticPattern;
}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
private<kafka> class WildcardTopicCount(ZkUtils zkUtils,
                                        String consumerIdString,
                                        TopicFilter topicFilter,
                                        Integer numStreams,
                                        Boolean excludeInternalTopics) extends TopicCount {
  public void  getConsumerThreadIdsPerTopic = {
    val wildcardTopics = zkUtils.getChildrenParentMayNotExist(ZkUtils.BrokerTopicsPath);
                         .filter(topic => topicFilter.isTopicAllowed(topic, excludeInternalTopics));
    TopicCount.makeConsumerThreadIdsPerTopic(consumerIdString, Map(wildcardTopics.map((_, numStreams)): _*));
  }

  public void  getTopicCountMap = Map(CoreUtils.JSONEscapeString(topicFilter.regex) -> numStreams);

  public void  String pattern = {
    topicFilter match {
      case Whitelist _ => TopicCount.whiteListPattern;
      case Blacklist _ => TopicCount.blackListPattern;
    }
  }

}

