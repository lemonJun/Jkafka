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

package kafka.producer;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import kafka.api._;
import kafka.network.{RequestOrResponseSend, BlockingChannel}
import kafka.utils._;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.Utils._;

@deprecated("This object has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.KafkaProducer instead.", "0.10.0.0");
object SyncProducer {
  val Short RequestKey = 0;
  val randomGenerator = new Random;
}

/*
 * Send a message set.
 */
@threadsafe
@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.KafkaProducer instead.", "0.10.0.0");
class SyncProducer(val SyncProducerConfig config) extends Logging {

  private val lock = new Object();
  @volatile private var Boolean shutdown = false
  private val blockingChannel = new BlockingChannel(config.host, config.port, BlockingChannel.UseDefaultBufferSize,
    config.sendBufferBytes, config.requestTimeoutMs);
  val producerRequestStats = ProducerRequestStatsRegistry.getProducerRequestStats(config.clientId);

  trace(String.format("Instantiating Scala Sync Producer with properties: %s",config.props))

  private public void  verifyRequest(RequestOrResponse request) = {
    /**
     * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
     * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
     * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
     */
    if (logger.isDebugEnabled) {
      val buffer = new RequestOrResponseSend("", request).buffer;
      trace("verifying sendbuffer of size " + buffer.limit)
      val requestTypeId = buffer.getShort();
      if(requestTypeId == ApiKeys.PRODUCE.id) {
        val request = ProducerRequest.readFrom(buffer);
        trace(request.toString);
      }
    }
  }

  /**
   * Common functionality for the public send methods
   */
  private public void  doSend(RequestOrResponse request, Boolean readResponse = true): NetworkReceive = {
    lock synchronized {
      verifyRequest(request)
      getOrMakeConnection();

      var NetworkReceive response = null;
      try {
        blockingChannel.send(request);
        if(readResponse)
          response = blockingChannel.receive();
        else;
          trace("Skipping reading response");
      } catch {
        case java e.io.IOException =>
          // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry;
          disconnect();
          throw e;
        case Throwable e => throw e;
      }
      response;
    }
  }

  /**
   * Send a message. If the producerRequest had required.request.acks=0, then the
   * returned response object is null
   */
  public void  send(ProducerRequest producerRequest): ProducerResponse = {
    val requestSize = producerRequest.sizeInBytes;
    producerRequestStats.getProducerRequestStats(config.host, config.port).requestSizeHist.update(requestSize);
    producerRequestStats.getProducerRequestAllBrokersStats.requestSizeHist.update(requestSize);

    var NetworkReceive response = null;
    val specificTimer = producerRequestStats.getProducerRequestStats(config.host, config.port).requestTimer;
    val aggregateTimer = producerRequestStats.getProducerRequestAllBrokersStats.requestTimer;
    aggregateTimer.time {
      specificTimer.time {
        response = doSend(producerRequest, producerRequest.requiredAcks != 0);
      }
    }
    if(producerRequest.requiredAcks != 0) {
      val producerResponse = ProducerResponse.readFrom(response.payload);
      producerRequestStats.getProducerRequestStats(config.host, config.port).throttleTimeStats.update(producerResponse.throttleTime, TimeUnit.MILLISECONDS);
      producerRequestStats.getProducerRequestAllBrokersStats.throttleTimeStats.update(producerResponse.throttleTime, TimeUnit.MILLISECONDS);
      producerResponse;
    }
    else;
      null;
  }

  public void  send(TopicMetadataRequest request): TopicMetadataResponse = {
    val response = doSend(request);
    TopicMetadataResponse.readFrom(response.payload);
  }

  public void  close() = {
    lock synchronized {
      disconnect();
      shutdown = true;
    }
  }

  /**
   * Disconnect from current channel, closing connection.
   * Side channel effect field is set to null on successful disconnect
   */
  private public void  disconnect() {
    try {
      info("Disconnecting from " + formatAddress(config.host, config.port))
      blockingChannel.disconnect();
    } catch {
      case Exception e => error("Error on disconnect: ", e);
    }
  }

  private public void  connect(): BlockingChannel = {
    if (!blockingChannel.isConnected && !shutdown) {
      try {
        blockingChannel.connect();
        info("Connected to " + formatAddress(config.host, config.port) + " for producing")
      } catch {
        case Exception e => {
          disconnect();
          error("Producer connection to " + formatAddress(config.host, config.port) + " unsuccessful", e)
          throw e;
        }
      }
    }
    blockingChannel;
  }

  private public void  getOrMakeConnection() {
    if(!blockingChannel.isConnected) {
      connect();
    }
  }
}
