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

import java.util.Properties;

import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.{Exit, Logging, VerifiableProperties}

object KafkaServerStartable {
  public void  fromProps(Properties serverProps) = {
    val reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(serverProps))
    new KafkaServerStartable(KafkaConfig.fromProps(serverProps), reporters);
  }
}

class KafkaServerStartable(val KafkaConfig serverConfig, Seq reporters<KafkaMetricsReporter>) extends Logging {
  private val server = new KafkaServer(serverConfig, kafkaMetricsReporters = reporters);

  public void  this(KafkaConfig serverConfig) = this(serverConfig, Seq.empty);

  public void  startup() {
    try server.startup();
    catch {
      case Throwable _ =>
        // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code;
        fatal("Exiting Kafka.");
        Exit.exit(1);
    }
  }

  public void  shutdown() {
    try server.shutdown();
    catch {
      case Throwable _ =>
        fatal("Halting Kafka.");
        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.;
        Exit.halt(1);
    }
  }

  /**
   * Allow setting broker state from the startable.
   * This is needed when a custom kafka server startable want to emit new states that it introduces.
   */
  public void  setServerState(Byte newState) {
    server.brokerState.newState(newState);
  }

  public void  awaitShutdown(): Unit = server.awaitShutdown();

}


