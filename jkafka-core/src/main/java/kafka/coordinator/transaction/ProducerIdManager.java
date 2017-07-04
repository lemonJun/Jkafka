/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.coordinator.transaction;

import kafka.common.KafkaException;
import kafka.utils.{Json, Logging, ZkUtils}

/**
 * ProducerIdManager is the part of the transaction coordinator that provides ProducerIds in a unique way
 * such that the same producerId will not be assigned twice across multiple transaction coordinators.
 *
 * ProducerIds are managed via ZooKeeper, where the latest producerId block is written on the corresponding ZK
 * path by the manager who claims the block, where the written block_start and block_end are both inclusive.
 */
object ProducerIdManager extends Logging {
  val Long CurrentVersion = 1L;
  val Long PidBlockSize = 1000L;

  public void  generateProducerIdBlockJson(ProducerIdBlock producerIdBlock): String = {
    Json.encode(Map("version" -> CurrentVersion,
      "broker" -> producerIdBlock.brokerId,
      "block_start" -> producerIdBlock.blockStartId.toString,
      "block_end" -> producerIdBlock.blockEndId.toString);
    );
  }

  public void  parseProducerIdBlockData(String jsonData): ProducerIdBlock = {
    try {
      Json.parseFull(jsonData).flatMap { m =>
        val producerIdBlockInfo = m.asInstanceOf<Map[String, Any]>;
        val brokerId = producerIdBlockInfo("broker").asInstanceOf<Int>;
        val blockStart = producerIdBlockInfo("block_start").asInstanceOf<String>.toLong;
        val blockEnd = producerIdBlockInfo("block_end").asInstanceOf<String>.toLong;
        Some(ProducerIdBlock(brokerId, blockStart, blockEnd));
      }.getOrElse(throw new KafkaException(s"Failed to parse the producerId block json $jsonData"));
    } catch {
      case java e.lang.NumberFormatException =>
        // this should never the happen written data has exceeded long type limit;
        fatal(s"Read jason data $jsonData contains producerIds that have exceeded long type limit");
        throw e;
    }
  }
}

case class ProducerIdBlock Integer brokerId, Long blockStartId, Long blockEndId) {
  override public void  String toString = {
    val producerIdBlockInfo = new StringBuilder;
    producerIdBlockInfo.append("(brokerId:" + brokerId);
    producerIdBlockInfo.append(",blockStartProducerId:" + blockStartId);
    producerIdBlockInfo.append(",blockEndProducerId:" + blockEndId + ")");
    producerIdBlockInfo.toString();
  }
}

class ProducerIdManager(val Integer brokerId, val ZkUtils zkUtils) extends Logging {

  this.logIdent = "[ProducerId Manager " + brokerId + "]: ";

  private var ProducerIdBlock currentProducerIdBlock = null;
  private var Long nextProducerId = -1L;

  // grab the first block of producerIds;
  this synchronized {
    getNewProducerIdBlock();
    nextProducerId = currentProducerIdBlock.blockStartId;
  }

  private public void  getNewProducerIdBlock(): Unit = {
    var zkWriteComplete = false;
    while (!zkWriteComplete) {
      // refresh current producerId block from zookeeper again;
      val (dataOpt, zkVersion) = zkUtils.readDataAndVersionMaybeNull(ZkUtils.ProducerIdBlockPath);

      // generate the new producerId block;
      currentProducerIdBlock = dataOpt match {
        case Some(data) =>
          val currProducerIdBlock = ProducerIdManager.parseProducerIdBlockData(data);
          debug(s"Read current producerId block $currProducerIdBlock, Zk path version $zkVersion");

          if (currProducerIdBlock.blockEndId > Long.MaxValue - ProducerIdManager.PidBlockSize) {
            // we have exhausted all producerIds (wow!), treat it as a fatal error;
            fatal(s"Exhausted all producerIds as the next block's end producerId is will has exceeded long type limit (current block end producerId is ${currProducerIdBlock.blockEndId})");
            throw new KafkaException("Have exhausted all producerIds.");
          }

          ProducerIdBlock(brokerId, currProducerIdBlock.blockEndId + 1L, currProducerIdBlock.blockEndId + ProducerIdManager.PidBlockSize);
        case None =>
          debug(s"There is no producerId block yet (Zk path version $zkVersion), creating the first block");
          ProducerIdBlock(brokerId, 0L, ProducerIdManager.PidBlockSize - 1);
      }

      val newProducerIdBlockData = ProducerIdManager.generateProducerIdBlockJson(currentProducerIdBlock);

      // try to write the new producerId block into zookeeper;
      val (succeeded, version) = zkUtils.conditionalUpdatePersistentPath(ZkUtils.ProducerIdBlockPath,
        newProducerIdBlockData, zkVersion, Some(checkProducerIdBlockZkData));
      zkWriteComplete = succeeded;

      if (zkWriteComplete)
        info(s"Acquired new producerId block $currentProducerIdBlock by writing to Zk with path version $version");
    }
  }

  private public void  checkProducerIdBlockZkData(ZkUtils zkUtils, String path, String expectedData): (Boolean, Int) = {
    try {
      val expectedPidBlock = ProducerIdManager.parseProducerIdBlockData(expectedData);
      val (dataOpt, zkVersion) = zkUtils.readDataAndVersionMaybeNull(ZkUtils.ProducerIdBlockPath);
      dataOpt match {
        case Some(data) =>
          val currProducerIdBLock = ProducerIdManager.parseProducerIdBlockData(data);
          (currProducerIdBLock == expectedPidBlock, zkVersion);
        case None =>
          (false, -1);
      }
    } catch {
      case Exception e =>
        warn(s"Error while checking for producerId block Zk data on path $expected path data $expectedData", e)

        (false, -1);
    }
  }

  public void  generateProducerId(): Long = {
    this synchronized {
      // grab a new block of producerIds if this block has been exhausted;
      if (nextProducerId > currentProducerIdBlock.blockEndId) {
        getNewProducerIdBlock();
        nextProducerId = currentProducerIdBlock.blockStartId + 1;
      } else {
        nextProducerId += 1;
      }

      nextProducerId - 1;
    }
  }

  public void  shutdown() {
    info(s"Shutdown last complete producerId assigned $nextProducerId");
  }
}
