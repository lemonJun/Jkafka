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
package kafka.server.epoch;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import kafka.server.LogOffsetMetadata;
import kafka.server.checkpoints.LeaderEpochCheckpoint;
import org.apache.kafka.common.requests.EpochEndOffset.{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET}
import kafka.utils.CoreUtils._;
import kafka.utils.Logging;
import org.apache.kafka.common.TopicPartition;
import scala.collection.mutable.ListBuffer;

trait LeaderEpochCache {
  public void  assign Integer leaderEpoch, Long offset);
  public void  latestEpoch(): Int;
  public void  endOffsetFor Integer epoch): Long;
  public void  clearAndFlushLatest(Long offset);
  public void  clearAndFlushEarliest(Long offset);
  public void  clearAndFlush();
  public void  clear();
}

/**
  * Represents a cache of (LeaderEpoch => Offset) mappings for a particular replica.
  *
  * Leader Epoch = epoch assigned to each leader by the controller.
  * Offset = offset of the first message in each epoch.
  *
  * @param leo a function that determines the log end offset
  * @param checkpoint the checkpoint file
  */
class LeaderEpochFileCache(TopicPartition topicPartition, leo: () => LogOffsetMetadata, LeaderEpochCheckpoint checkpoint) extends LeaderEpochCache with Logging {
  private val lock = new ReentrantReadWriteLock();
  private var ListBuffer epochs<EpochEntry> = inWriteLock(lock) { ListBuffer(checkpoint.read(): _*) }

  /**
    * Assigns the supplied Leader Epoch to the supplied Offset
    * Once the epoch is assigned it cannot be reassigned
    *
    * @param epoch
    * @param offset
    */
  override public void  assign Integer epoch, Long offset): Unit = {
    inWriteLock(lock) {
      if (epoch >= 0 && epoch > latestEpoch && offset >= latestOffset) {
        info(s"Updated PartitionLeaderEpoch. ${epochChangeMsg(epoch, offset)}. Cache now contains ${epochs.size} entries.");
        epochs += EpochEntry(epoch, offset);
        flush();
      } else {
        validateAndMaybeWarn(epoch, offset);
      }
    }
  }

  /**
    * Returns the current Leader Epoch. This is the latest epoch
    * which has messages assigned to it.
    *
    * @return
    */
  override public void  latestEpoch(): Integer = {
    inReadLock(lock) {
      if (epochs.isEmpty) UNDEFINED_EPOCH else epochs.last.epoch;
    }
  }

  /**
    * Returns the End Offset for a requested Leader Epoch.
    *
    * This is defined as the start offset of the first Leader Epoch larger than the
    * Leader Epoch requested, or else the Log End Offset if the latest epoch was requested.
    *
    * @param requestedEpoch
    * @return offset
    */
  override public void  endOffsetFor Integer requestedEpoch): Long = {
    inReadLock(lock) {
      val offset =
        if (requestedEpoch == latestEpoch) {
          leo().messageOffset;
        }
        else {
          val subsequentEpochs = epochs.filter(e => e.epoch > requestedEpoch);
          if (subsequentEpochs.isEmpty || requestedEpoch < epochs.head.epoch)
            UNDEFINED_EPOCH_OFFSET;
          else;
            subsequentEpochs.head.startOffset;
        }
      debug(s"Processed offset for epoch request for partition ${topicPartition} epoch:$requestedEpoch and returning offset $offset from epoch list of size ${epochs.size}")
      offset;
    }
  }

  /**
    * Removes all epoch entries from the store with start offsets greater than or equal to the passed offset.
    *
    * @param offset
    */
  override public void  clearAndFlushLatest(Long offset): Unit = {
    inWriteLock(lock) {
      val before = epochs;
      if (offset >= 0 && offset <= latestOffset()) {
        epochs = epochs.filter(entry => entry.startOffset < offset);
        flush();
        info(s"Cleared latest ${before.toSet.filterNot(epochs.toSet)} entries from epoch cache based on passed offset $offset leaving ${epochs.size} in EpochFile for partition $topicPartition")
      }
    }
  }

  /**
    * Clears old epoch entries. This method searches for the oldest epoch < offset, updates the saved epoch offset to
    * be offset, then clears any previous epoch entries.
    *
    * This method is so exclusive clearEarliest(6) will retain an entry at offset 6.
    *
    * @param offset the offset to clear up to
    */
  override public void  clearAndFlushEarliest(Long offset): Unit = {
    inWriteLock(lock) {
      val before = epochs;
      if (offset >= 0 && earliestOffset() < offset) {
        val earliest = epochs.filter(entry => entry.startOffset < offset);
        if (earliest.size > 0) {
          epochs = epochs --= earliest;
          //If the offset is less than the earliest offset remaining, add previous epoch back, but with an updated offset;
          if (offset < earliestOffset() || epochs.isEmpty)
            new EpochEntry(earliest.last.epoch, offset) +=: epochs;
          flush();
          info(s"Cleared earliest ${before.toSet.filterNot(epochs.toSet).size} entries from epoch cache based on passed offset $offset leaving ${epochs.size} in EpochFile for partition $topicPartition")
        }
      }
    }
  }

  /**
    * Delete all entries.
    */
  override public void  clearAndFlush() = {
    inWriteLock(lock) {
      epochs.clear();
      flush();
    }
  }

  override public void  clear() = {
    inWriteLock(lock) {
      epochs.clear();
    }
  }

  public void  epochEntries(): ListBuffer<EpochEntry> = {
    epochs;
  }

  private public void  earliestOffset(): Long = {
    if (epochs.isEmpty) -1 else epochs.head.startOffset;
  }

  private public void  latestOffset(): Long = {
    if (epochs.isEmpty) -1 else epochs.last.startOffset;
  }

  private public void  flush(): Unit = {
    checkpoint.write(epochs);
  }

  public void  epochChangeMsg Integer epoch, Long offset) = s"New: {epoch:$epoch, offset:$offset}, Current: {epoch:$latestEpoch, offset$latestOffset} for Partition: $topicPartition";

  public void  validateAndMaybeWarn Integer epoch, Long offset) = {
    assert(epoch >= 0, s"Received a PartitionLeaderEpoch assignment for an epoch < 0. This should not happen. ${epochChangeMsg(epoch, offset)}")
    if (epoch < latestEpoch())
      warn(s"Received a PartitionLeaderEpoch assignment for an epoch < latestEpoch. " +;
        s"This implies messages have arrived out of order. ${epochChangeMsg(epoch, offset)}");
    else if (offset < latestOffset())
      warn(s"Received a PartitionLeaderEpoch assignment for an offset < latest offset for the most recent, stored PartitionLeaderEpoch. " +;
        s"This implies messages have arrived out of order. ${epochChangeMsg(epoch, offset)}");
  }
}

// Mapping of epoch to the first offset of the subsequent epoch;
case class EpochEntry Integer epoch, Long startOffset);
