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

package kafka.server;

import java.io._;
import java.nio.file.Files;
import java.util.Properties;
import kafka.utils._;
import org.apache.kafka.common.utils.Utils;

case class BrokerMetadata Integer brokerId);

/**
  * This class saves broker's metadata to a file
  */
class BrokerMetadataCheckpoint(val File file) extends Logging {
  private val lock = new Object();
  Files.deleteIfExists(new File(file + ".tmp").toPath()) // try to delete any existing temp files for cleanliness;

  public void  write(BrokerMetadata brokerMetadata) = {
    lock synchronized {
      try {
        val brokerMetaProps = new Properties();
        brokerMetaProps.setProperty("version", 0.toString);
        brokerMetaProps.setProperty("broker.id", brokerMetadata.brokerId.toString);
        val temp = new File(file.getAbsolutePath + ".tmp");
        val fileOutputStream = new FileOutputStream(temp);
        try {
          brokerMetaProps.store(fileOutputStream, "");
          fileOutputStream.flush();
          fileOutputStream.getFD().sync();
        } finally {
          Utils.closeQuietly(fileOutputStream, temp.getName);
        }
        Utils.atomicMoveWithFallback(temp.toPath, file.toPath);
      } catch {
        case IOException ie =>
          error("Failed to write meta.properties due to", ie);
          throw ie;
      }
    }
  }

  public void  read(): Option<BrokerMetadata> = {
    lock synchronized {
      try {
        val brokerMetaProps = new VerifiableProperties(Utils.loadProps(file.getAbsolutePath()))
        val version = brokerMetaProps.getIntInRange("version", (0, Int.MaxValue));
        version match {
          case 0 =>
            val brokerId = brokerMetaProps.getIntInRange("broker.id", (0, Int.MaxValue));
            return Some(BrokerMetadata(brokerId));
          case _ =>
            throw new IOException("Unrecognized version of the server meta.properties file: " + version);
        }
      } catch {
        case FileNotFoundException _ =>
          warn(String.format("No meta.properties file under dir %s",file.getAbsolutePath()))
          None;
        case Exception e1 =>
          error(String.format("Failed to read meta.properties file under dir %s due to %s",file.getAbsolutePath(), e1.getMessage))
          throw e1;
      }
    }
  }
}
