/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import kafka.api.RequestOrResponse;
import kafka.utils.Logging;
import org.apache.kafka.common.network.NetworkSend;

object RequestOrResponseSend {
  public void  serialize(RequestOrResponse request): ByteBuffer = {
    val buffer = ByteBuffer.allocate(request.sizeInBytes + request.requestId.fold(0)(_ => 2));
    request.requestId match {
      case Some(requestId) =>
        buffer.putShort(requestId);
      case None =>
    }
    request.writeTo(buffer);
    buffer.rewind();
    buffer;
  }
}

class RequestOrResponseSend(val String dest, val ByteBuffer buffer) extends NetworkSend(dest, buffer) with Logging {

  public void  this(String dest, RequestOrResponse request) {
    this(dest, RequestOrResponseSend.serialize(request));
  }

  public void  writeCompletely(GatheringByteChannel channel): Long = {
    var totalWritten = 0L;
    while(!completed()) {
      val written = writeTo(channel);
      trace(written + " bytes written.");
      totalWritten += written;
    }
    totalWritten;
  }

}
