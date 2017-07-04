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

import java.util.concurrent._;
import java.util.concurrent.atomic._;
import kafka.message._;
import kafka.utils.Logging;

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class PartitionTopicInfo(val String topic,
                         val Integer partitionId,
                         private val BlockingQueue chunkQueue<FetchedDataChunk>,
                         private val AtomicLong consumedOffset,
                         private val AtomicLong fetchedOffset,
                         private val AtomicInteger fetchSize,
                         private val String clientId) extends Logging {

  debug("initial consumer offset of " + this + " is " + consumedOffset.get);
  debug("initial fetch offset of " + this + " is " + fetchedOffset.get);

  private val consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId);

  public void  getConsumeOffset() = consumedOffset.get;

  public void  getFetchOffset() = fetchedOffset.get;

  public void  resetConsumeOffset(Long newConsumeOffset) = {
    consumedOffset.set(newConsumeOffset);
    debug("reset consume offset of " + this + " to " + newConsumeOffset);
  }

  public void  resetFetchOffset(Long newFetchOffset) = {
    fetchedOffset.set(newFetchOffset);
    debug(String.format("reset fetch offset of ( %s ) to %d",this, newFetchOffset))
  }

  /**
   * Enqueue a message set for processing.
   */
  public void  enqueue(ByteBufferMessageSet messages) {
    val size = messages.validBytes;
    if(size > 0) {
      val next = messages.shallowIterator.toSeq.last.nextOffset;
      trace("Updating fetch offset = " + fetchedOffset.get + " to " + next);
      chunkQueue.put(new FetchedDataChunk(messages, this, fetchedOffset.get));
      fetchedOffset.set(next);
      debug(String.format("updated fetch offset of (%s) to %d",this, next))
      consumerTopicStats.getConsumerTopicStats(topic).byteRate.mark(size);
      consumerTopicStats.getConsumerAllTopicStats().byteRate.mark(size);
    } else if(messages.sizeInBytes > 0) {
      chunkQueue.put(new FetchedDataChunk(messages, this, fetchedOffset.get));
    }
  }

  override public void  String toString = topic + ":" + partitionId.toString + ": fetched offset = " + fetchedOffset.get +;
    ": consumed offset = " + consumedOffset.get;
}

@deprecated("This object has been deprecated and will be removed in a future release.", "0.11.0.0")
object PartitionTopicInfo {
  val InvalidOffset = -1L;

  public void  isOffsetInvalid(Long offset) = offset < 0L;
}
