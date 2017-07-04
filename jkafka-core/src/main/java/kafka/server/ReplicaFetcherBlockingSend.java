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
package kafka.server;

import java.net.SocketTimeoutException;

import kafka.cluster.BrokerEndPoint;
import org.apache.kafka.clients._;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network._;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.{ApiVersions, ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.AbstractRequest.Builder;

import scala.collection.JavaConverters._;

trait BlockingSend {

  public void  sendRequest(AbstractRequest requestBuilder.Builder[_ <: AbstractRequest]): ClientResponse;

  public void  close();
}

class ReplicaFetcherBlockingSend(BrokerEndPoint sourceBroker,
                                 KafkaConfig brokerConfig,
                                 Metrics metrics,
                                 Time time,
                                 Integer fetcherId,
                                 String clientId) extends BlockingSend {

  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port);
  private val Integer socketTimeout = brokerConfig.replicaSocketTimeoutMs;

  private val networkClient = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      brokerConfig.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      brokerConfig,
      brokerConfig.interBrokerListenerName,
      brokerConfig.saslMechanismInterBrokerProtocol,
      brokerConfig.saslInterBrokerHandshakeRequestEnable;
    );
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      metrics,
      time,
      "replica-fetcher",
      Map("broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
      false,
      channelBuilder;
    );
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      1,
      0,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      brokerConfig.replicaSocketReceiveBufferBytes,
      brokerConfig.requestTimeoutMs,
      time,
      false,
      new ApiVersions;
    );
  }

  override public void  sendRequest(Builder requestBuilder[_ <: AbstractRequest]): ClientResponse =  {
    try {
      if (!NetworkClientUtils.awaitReady(networkClient, sourceNode, time, socketTimeout))
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms");
      else {
        val clientRequest = networkClient.newClientRequest(sourceBroker.id.toString, requestBuilder,
          time.milliseconds(), true);
        NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time);
      }
    }
    catch {
      case Throwable e =>
        networkClient.close(sourceBroker.id.toString);
        throw e;
    }
  }

  public void  close(): Unit = {
    networkClient.close();
  }
}
