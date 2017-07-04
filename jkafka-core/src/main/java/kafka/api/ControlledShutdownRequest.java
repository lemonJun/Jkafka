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

import java.nio.ByteBuffer;

import kafka.common.TopicAndPartition;
import kafka.api.ApiUtils._;
import kafka.network.{RequestOrResponseSend, RequestChannel}
import kafka.network.RequestChannel.Response;
import kafka.utils.Logging;
import org.apache.kafka.common.protocol.{ApiKeys, Errors}

object ControlledShutdownRequest extends Logging {
  val CurrentVersion = 1.shortValue;
  val DefaultClientId = "";

  public void  readFrom(ByteBuffer buffer): ControlledShutdownRequest = {
    val versionId = buffer.getShort;
    val correlationId = buffer.getInt;
    val clientId = if (versionId > 0) Some(readShortString(buffer)) else None;
    val brokerId = buffer.getInt;
    new ControlledShutdownRequest(versionId, correlationId, clientId, brokerId);
  }

}

case class ControlledShutdownRequest(Short versionId,
                                     Integer correlationId,
                                     Option clientId<String>,
                                     Integer brokerId);
  extends RequestOrResponse(Some(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id)){

  if (versionId > 0 && clientId.isEmpty)
    throw new IllegalArgumentException("`clientId` must be defined if `versionId` > 0")

  public void  writeTo(ByteBuffer buffer) {
    buffer.putShort(versionId);
    buffer.putInt(correlationId);
    clientId.foreach(writeShortString(buffer, _))
    buffer.putInt(brokerId);
  }

  public void  Integer sizeInBytes = {
    2 + /* version id */
      4 + /* correlation id */
      clientId.fold(0)(shortStringLength) +;
      4 /* broker id */
  }

  override public void  String toString = {
    describe(true);
  }

  override public void  handleError(Throwable e, RequestChannel requestChannel, RequestChannel request.Request): Unit = {
    val errorResponse = ControlledShutdownResponse(correlationId, Errors.forException(e), Set.empty<TopicAndPartition>)
    requestChannel.sendResponse(Response(request, new RequestOrResponseSend(request.connectionId, errorResponse)));
  }

  override public void  describe(Boolean details = false): String = {
    val controlledShutdownRequest = new StringBuilder;
    controlledShutdownRequest.append("Name: " + this.getClass.getSimpleName);
    controlledShutdownRequest.append("; Version: " + versionId);
    controlledShutdownRequest.append("; CorrelationId: " + correlationId);
    controlledShutdownRequest.append(";ClientId:" + clientId);
    controlledShutdownRequest.append("; BrokerId: " + brokerId);
    controlledShutdownRequest.toString();
  }
}
