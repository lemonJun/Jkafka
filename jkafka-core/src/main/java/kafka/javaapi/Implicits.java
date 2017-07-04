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

import kafka.utils.Logging;

private<javaapi> object Implicits extends Logging {

  implicit public void  scalaMessageSetToJavaMessageSet(kafka messageSet.message.ByteBufferMessageSet):;
     kafka.javaapi.message.ByteBufferMessageSet = {
    new kafka.javaapi.message.ByteBufferMessageSet(messageSet.buffer);
  }

  implicit public void  toJavaFetchResponse(kafka response.api.FetchResponse): kafka.javaapi.FetchResponse =
    new kafka.javaapi.FetchResponse(response);

  implicit public void  toJavaTopicMetadataResponse(kafka response.api.TopicMetadataResponse): kafka.javaapi.TopicMetadataResponse =
    new kafka.javaapi.TopicMetadataResponse(response);

  implicit public void  toJavaOffsetResponse(kafka response.api.OffsetResponse): kafka.javaapi.OffsetResponse =
    new kafka.javaapi.OffsetResponse(response);

  implicit public void  toJavaOffsetFetchResponse(kafka response.api.OffsetFetchResponse): kafka.javaapi.OffsetFetchResponse =
    new kafka.javaapi.OffsetFetchResponse(response);

  implicit public void  toJavaOffsetCommitResponse(kafka response.api.OffsetCommitResponse): kafka.javaapi.OffsetCommitResponse =
    new kafka.javaapi.OffsetCommitResponse(response);

  implicit public void  optionToJavaRef<T](Option opt[T>): T = {
    opt match {
      case Some(obj) => obj;
      case None => null.asInstanceOf[T];
    }
  }

}