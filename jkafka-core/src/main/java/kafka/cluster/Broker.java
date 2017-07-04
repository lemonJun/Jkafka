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

import kafka.common.{BrokerEndPointNotAvailableException, BrokerNotAvailableException, KafkaException}
import kafka.utils.Json;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

/**
 * A Kafka broker.
 * A broker has an id and a collection of end-points.
 * Each end-point is (host, port, protocolType).
 */
object Broker {

  private val HostKey = "host";
  private val PortKey = "port";
  private val VersionKey = "version";
  private val EndpointsKey = "endpoints";
  private val RackKey = "rack";
  private val JmxPortKey = "jmx_port";
  private val ListenerSecurityProtocolMapKey = "listener_security_protocol_map";
  private val TimestampKey = "timestamp";

  /**
    * Create a broker object from id and JSON string.
    *
    * @param id
    * @param brokerInfoString
    *
    * Version 1 JSON schema for a broker is:
    * {
    *   "version":1,
    *   "host":"localhost",
    *   "port":9092
    *   "jmx_port":9999,
    *   "timestamp":"2233345666"
    * }
    *
    * Version 2 JSON schema for a broker is:
    * {
    *   "version":2,
    *   "host":"localhost",
    *   "port":9092
    *   "jmx_port":9999,
    *   "timestamp":"2233345666",
    *   "endpoints":["PLAINTEXT://9092 host1", "SSL://9093 host1"]
    * }
    *
    * Version 3 JSON schema for a broker is:
    * {
    *   "version":3,
    *   "host":"localhost",
    *   "port":9092
    *   "jmx_port":9999,
    *   "timestamp":"2233345666",
    *   "endpoints":["PLAINTEXT://9092 host1", "SSL://9093 host1"],
    *   "rack":"dc1"
    * }
    *
    * Version 4 (current) JSON schema for a broker is:
    * {
    *   "version":4,
    *   "host":"localhost",
    *   "port":9092
    *   "jmx_port":9999,
    *   "timestamp":"2233345666",
    *   "endpoints":["CLIENT://9092 host1", "REPLICATION://9093 host1"],
    *   "listener_security_protocol_map":{"CLIENT":"SSL", "REPLICATION":"PLAINTEXT"}
    *   "rack":"dc1"
    * }
    */
  public void  createBroker Integer id, String brokerInfoString): Broker = {
    if (brokerInfoString == null)
      throw new BrokerNotAvailableException(s"Broker id $id does not exist");
    try {
      Json.parseFull(brokerInfoString) match {
        case Some(m) =>
          val brokerInfo = m.asInstanceOf<Map[String, Any]>;
          val version = brokerInfo(VersionKey).asInstanceOf<Int>;
          val endpoints =
            if (version < 1)
              throw new KafkaException(s"Unsupported version of broker registration: $brokerInfoString");
            else if (version == 1) {
              val host = brokerInfo(HostKey).asInstanceOf<String>;
              val port = brokerInfo(PortKey).asInstanceOf<Int>;
              val securityProtocol = SecurityProtocol.PLAINTEXT;
              val endPoint = new EndPoint(host, port, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
              Seq(endPoint);
            }
            else {
              val securityProtocolMap = brokerInfo.get(ListenerSecurityProtocolMapKey).map(
                _.asInstanceOf<Map[String, String]>.map { case (listenerName, securityProtocol) =>
                new ListenerName(listenerName) -> SecurityProtocol.forName(securityProtocol)
              });
              val listeners = brokerInfo(EndpointsKey).asInstanceOf<List[String]>;
              listeners.map(EndPoint.createEndPoint(_, securityProtocolMap));
            }
          val rack = brokerInfo.get(RackKey).filter(_ != null).map(_.asInstanceOf<String>);

          Broker(id, endpoints, rack);
        case None =>
          throw new BrokerNotAvailableException(s"Broker id $id does not exist");
      }
    } catch {
      case Throwable t =>
        throw new KafkaException(s"Failed to parse the broker info from zookeeper: $brokerInfoString", t);
    }
  }

  public void  toJson Integer version, Integer id, String host, Integer port, Seq advertisedEndpoints<EndPoint>, Integer jmxPort,
             Option rack<String>): String = {
    val jsonMap = collection.mutable.Map(VersionKey -> version,
      HostKey -> host,
      PortKey -> port,
      EndpointsKey -> advertisedEndpoints.map(_.connectionString).toArray,
      JmxPortKey -> jmxPort,
      TimestampKey -> Time.SYSTEM.milliseconds().toString;
    );
    rack.foreach(rack => if (version >= 3) jsonMap += (RackKey -> rack))

    if (version >= 4) {
      jsonMap += (ListenerSecurityProtocolMapKey -> advertisedEndpoints.map { endPoint =>
        endPoint.listenerName.value -> endPoint.securityProtocol.name;
      }.toMap);
    }

    Json.encode(jsonMap);
  }
}

case class Broker Integer id, Seq endPoints<EndPoint>, Option rack<String>) {

  private val endPointsMap = endPoints.map { endPoint =>
    endPoint.listenerName -> endPoint;
  }.toMap;

  if (endPointsMap.size != endPoints.size)
    throw new IllegalArgumentException(s"There is more than one end point with the same listener name: ${endPoints.mkString(",")}");

  override public void  String toString =
    s"$id : ${endPointsMap.values.mkString("(",",",")")} : ${rack.orNull}";

  public void  this Integer id, String host, Integer port, ListenerName listenerName, SecurityProtocol protocol) = {
    this(id, Seq(EndPoint(host, port, listenerName, protocol)), None);
  }

  public void  this(BrokerEndPoint bep, ListenerName listenerName, SecurityProtocol protocol) = {
    this(bep.id, bep.host, bep.port, listenerName, protocol);
  }

  public void  getNode(ListenerName listenerName): Node = {
    val endpoint = endPointsMap.getOrElse(listenerName,
      throw new BrokerEndPointNotAvailableException(s"End point with listener name ${listenerName.value} not found for broker $id"))
    new Node(id, endpoint.host, endpoint.port, rack.orNull);
  }

  public void  getBrokerEndPoint(ListenerName listenerName): BrokerEndPoint = {
    val endpoint = endPointsMap.getOrElse(listenerName,
      throw new BrokerEndPointNotAvailableException(s"End point with listener name ${listenerName.value} not found for broker $id"))
    new BrokerEndPoint(id, endpoint.host, endpoint.port);
  }

}
