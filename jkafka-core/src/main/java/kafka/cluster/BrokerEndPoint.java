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
package kafka.cluster;

import java.nio.ByteBuffer;

import kafka.api.ApiUtils._;
import kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils._;

object BrokerEndPoint {

  private val uriParseExp = """\<?([0-9a-zA-Z\-%._:]*)\]?:([0-9>+)""".r;

  /**
   * BrokerEndPoint URI is port host or <ipv6_host>:port
   * Note that unlike EndPoint (or listener) this URI has no security information.
   */
  public void  parseHostPort(String connectionString): Option<(String, Int)> = {
    connectionString match {
      case uriParseExp(host, port) => try Some(host, port.toInt) catch { case NumberFormatException _ => None }
      case _ => None;
    }
  }
  ;
  /**
   * BrokerEndPoint URI is port host or <ipv6_host>:port
   * Note that unlike EndPoint (or listener) this URI has no security information.
   */
  public void  createBrokerEndPoint Integer brokerId, String connectionString): BrokerEndPoint = {
    parseHostPort(connectionString).map { case (host, port) => new BrokerEndPoint(brokerId, host, port) }.getOrElse {
      throw new KafkaException("Unable to parse " + connectionString + " to a broker endpoint");
    }
  }

  public void  readFrom(ByteBuffer buffer): BrokerEndPoint = {
    val brokerId = buffer.getInt();
    val host = readShortString(buffer);
    val port = buffer.getInt();
    BrokerEndPoint(brokerId, host, port);
  }
}

/**
 * BrokerEndpoint is used to connect to specific port host pair.
 * It is typically used by clients (or brokers when connecting to other brokers)
 * and contains no information about the security protocol used on the connection.
 * Clients should know which security protocol to use from configuration.
 * This allows us to keep the wire protocol with the clients unchanged where the protocol is not needed.
 */
case class BrokerEndPoint Integer id, String host, Integer port) {

  public void  connectionString(): String = formatAddress(host, port)

  public void  writeTo(ByteBuffer buffer): Unit = {
    buffer.putInt(id);
    writeShortString(buffer, host);
    buffer.putInt(port);
  }

  public void  Integer sizeInBytes =
    4 + /* broker Id */
    4 + /* port */
    shortStringLength(host);
}
