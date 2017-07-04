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
package kafka.api;

import java.nio._;
import kafka.common._;

/**
 * Helper functions specific to parsing or serializing requests and responses
 */
object ApiUtils {
  ;
  val ProtocolEncoding = "UTF-8";

    /**
   * Read size prefixed string where the size is stored as a 2 byte short.
   * @param buffer The buffer to read from
   */
  public void  readShortString(ByteBuffer buffer): String = {
    val Integer size = buffer.getShort();
    if(size < 0)
      return null;
    val bytes = new Array<Byte>(size);
    buffer.get(bytes);
    new String(bytes, ProtocolEncoding);
  }
  ;
  /**
   * Write a size prefixed string where the size is stored as a 2 byte short
   * @param buffer The buffer to write to
   * @param string The string to write
   */
  public void  writeShortString(ByteBuffer buffer, String string) {
    if(string == null) {
      buffer.putShort(-1);
    } else {
      val encodedString = string.getBytes(ProtocolEncoding);
      if(encodedString.length > Short.MaxValue) {
        throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".");
      } else {
        buffer.putShort(encodedString.length.asInstanceOf<Short>);
        buffer.put(encodedString);
      }
    }
  }
  ;
  /**
   * Return size of a size prefixed string where the size is stored as a 2 byte short
   * @param string The string to write
   */
  public void  shortStringLength(String string): Integer = {
    if(string == null) {
      2;
    } else {
      val encodedString = string.getBytes(ProtocolEncoding);
      if(encodedString.length > Short.MaxValue) {
        throw new KafkaException("String exceeds the maximum size of " + Short.MaxValue + ".");
      } else {
        2 + encodedString.length;
      }
    }
  }
  ;
  /**
   * Read an integer out of the bytebuffer from the current position and check that it falls within the given
   * range. If not, throw KafkaException.
   */
  public void  readIntInRange(ByteBuffer buffer, String name, range: (Int, Int)): Integer = {
    val value = buffer.getInt;
    if(value < range._1 || value > range._2)
      throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".");
    else value;
  }

  /**
   * Read a short out of the bytebuffer from the current position and check that it falls within the given
   * range. If not, throw KafkaException.
   */
  public void  readShortInRange(ByteBuffer buffer, String name, range: (Short, Short)): Short = {
    val value = buffer.getShort;
    if(value < range._1 || value > range._2)
      throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".");
    else value;
  }
  ;
}
