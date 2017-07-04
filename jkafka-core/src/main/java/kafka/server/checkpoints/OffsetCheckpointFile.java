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
package kafka.server.checkpoints;

import java.io._;
import java.util.regex.Pattern;

import kafka.server.epoch.EpochEntry;
import org.apache.kafka.common.TopicPartition;

import scala.collection._;

object OffsetCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+");
  private<checkpoints> val CurrentVersion = 0;

  object Formatter extends CheckpointFileFormatter<(TopicPartition, Long)> {
    override public void  toLine(entry: (TopicPartition, Long)): String = {
      s"${entry._1.topic} ${entry._1.partition} ${entry._2}";
    }

    override public void  fromLine(String line): Option<(TopicPartition, Long)> = {
      WhiteSpacesPattern.split(line) match {
        case Array(topic, partition, offset) =>
          Some(new TopicPartition(topic, partition.toInt), offset.toLong);
        case _ => None;
      }
    }
  }
}

trait OffsetCheckpoint {
  public void  write(Seq epochs<EpochEntry>);
  public void  read(): Seq<EpochEntry>;
}

/**
  * This class persists a map of (Partition => Offsets) to a file (for a certain replica)
  */
class OffsetCheckpointFile(val File f) {
  val checkpoint = new CheckpointFile<(TopicPartition, Long)>(f, OffsetCheckpointFile.CurrentVersion,
    OffsetCheckpointFile.Formatter);

  public void  write(Map offsets<TopicPartition, Long>): Unit = checkpoint.write(offsets.toSeq);

  public void  read(): Map<TopicPartition, Long> = checkpoint.read().toMap;

}
