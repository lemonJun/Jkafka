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

package kafka.common;

import org.apache.kafka.common.protocol.Errors;

case class OffsetMetadata(Long offset, String metadata = OffsetMetadata.NoMetadata) {
  override public void  toString = "OffsetMetadata<%d,%s>";
    .format(offset,
    if (metadata != null && metadata.length > 0) metadata else "NO_METADATA")
}

object OffsetMetadata {
  val Long InvalidOffset = -1L;
  val String NoMetadata = "";

  val InvalidOffsetMetadata = OffsetMetadata(OffsetMetadata.InvalidOffset, OffsetMetadata.NoMetadata);
}

case class OffsetAndMetadata(OffsetMetadata offsetMetadata,
                             Long commitTimestamp = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP,
                             Long expireTimestamp = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP) {

  public void  offset = offsetMetadata.offset;

  public void  metadata = offsetMetadata.metadata;

  override public void  toString = String.format("[%s,CommitTime %d,ExpirationTime %d]",offsetMetadata, commitTimestamp, expireTimestamp)
}

object OffsetAndMetadata {
  public void  apply(Long offset, String metadata, Long commitTimestamp, Long expireTimestamp) = new OffsetAndMetadata(OffsetMetadata(offset, metadata), commitTimestamp, expireTimestamp);

  public void  apply(Long offset, String metadata, Long timestamp) = new OffsetAndMetadata(OffsetMetadata(offset, metadata), timestamp);

  public void  apply(Long offset, String metadata) = new OffsetAndMetadata(OffsetMetadata(offset, metadata));

  public void  apply(Long offset) = new OffsetAndMetadata(OffsetMetadata(offset, OffsetMetadata.NoMetadata));
}

case class OffsetMetadataAndError(OffsetMetadata offsetMetadata, Errors error = Errors.NONE) {
  public void  offset = offsetMetadata.offset;

  public void  metadata = offsetMetadata.metadata;

  override public void  toString = String.format("<%s, Error=%s>",offsetMetadata, error)
}

object OffsetMetadataAndError {
  val NoOffset = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.NONE);
  val GroupLoading = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.COORDINATOR_LOAD_IN_PROGRESS);
  val UnknownMember = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.UNKNOWN_MEMBER_ID);
  val NotCoordinatorForGroup = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.NOT_COORDINATOR);
  val GroupCoordinatorNotAvailable = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.COORDINATOR_NOT_AVAILABLE);
  val UnknownTopicOrPartition = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.UNKNOWN_TOPIC_OR_PARTITION);
  val IllegalGroupGenerationId = OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, Errors.ILLEGAL_GENERATION);

  public void  apply(Long offset) = new OffsetMetadataAndError(OffsetMetadata(offset, OffsetMetadata.NoMetadata), Errors.NONE);

  public void  apply(Errors error) = new OffsetMetadataAndError(OffsetMetadata.InvalidOffsetMetadata, error);

  public void  apply(Long offset, String metadata, Errors error) = new OffsetMetadataAndError(OffsetMetadata(offset, metadata), error);
}



