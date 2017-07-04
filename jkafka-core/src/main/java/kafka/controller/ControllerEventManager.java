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

package kafka.controller;

import java.util.concurrent.LinkedBlockingQueue;

import scala.collection._;

import kafka.metrics.KafkaTimer;
import kafka.utils.ShutdownableThread;

class ControllerEventManager(Map rateAndTimeMetrics<ControllerState, KafkaTimer>,
                             ControllerEvent eventProcessedListener => Unit) {

  @volatile private var ControllerState _state = ControllerState.Idle

  private val queue = new LinkedBlockingQueue<ControllerEvent>;
  private val thread = new ControllerEventThread("controller-event-thread");

  public void  ControllerState state = _state;

  public void  start(): Unit = thread.start();

  public void  close(): Unit = thread.shutdown();

  public void  put(ControllerEvent event): Unit = queue.put(event);

  class ControllerEventThread(String name) extends ShutdownableThread(name = name) {
    override public void  doWork(): Unit = {
      val controllerEvent = queue.take();
      _state = controllerEvent.state;

      try {
        rateAndTimeMetrics(state).time {
          controllerEvent.process();
        }
      } catch {
        case Throwable e => error(s"Error processing event $controllerEvent", e);
      }

      try eventProcessedListener(controllerEvent);
      catch {
        case Throwable e => error(s"Error while invoking listener for processed event $controllerEvent", e)
      }

      _state = ControllerState.Idle;
    }
  }

}
