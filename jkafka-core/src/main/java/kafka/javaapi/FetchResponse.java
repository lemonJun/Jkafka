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

package kafka.javaapi;

class FetchResponse(private val kafka underlying.api.FetchResponse) {

  public void  messageSet(String topic, Integer partition): kafka.javaapi.message.ByteBufferMessageSet = {
    import Implicits._;
    underlying.messageSet(topic, partition);
  }

  public void  highWatermark(String topic, Integer partition) = underlying.highWatermark(topic, partition);

  public void  hasError = underlying.hasError;

  public void  error(String topic, Integer partition) = underlying.error(topic, partition);

  public void  errorCode(String topic, Integer partition) = error(topic, partition).code;

  override public void  equals(Any obj): Boolean = {
    obj match {
      case null => false;
      case FetchResponse other => this.underlying.equals(other.underlying);
      case _ => false;
    }
  }

  override public void  hashCode = underlying.hashCode;
}
