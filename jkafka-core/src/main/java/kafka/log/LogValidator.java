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
package kafka.log;

import java.nio.ByteBuffer;

import kafka.common.LongRef;
import kafka.message.{CompressionCodec, NoCompressionCodec}
import kafka.utils.Logging;
import org.apache.kafka.common.errors.{InvalidTimestampException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._;

import scala.collection.mutable;
import scala.collection.JavaConverters._;

private<kafka> object LogValidator extends Logging {

  /**
   * Update the offsets for this message set and do further validation on messages including:
   * 1. Messages for compacted topics must have keys
   * 2. When magic value >= 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0.
   * 3. When magic value >= 1, validate and maybe overwrite timestamps of messages.
   * 4. Declared count of records in DefaultRecordBatch must match number of valid records contained therein.
   *
   * This method will convert messages as necessary to the topic's configured message format version. If no format
   * conversion or value overwriting is required for messages, this method will perform in-place operations to
   * avoid expensive re-compression.
   *
   * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp, the offset
   * of the shallow message with the max timestamp and a boolean indicating whether the message sizes may have changed.
   */
  private<kafka> public void  validateMessagesAndAssignOffsets(MemoryRecords records,
                                                      LongRef offsetCounter,
                                                      Long now,
                                                      CompressionCodec sourceCodec,
                                                      CompressionCodec targetCodec,
                                                      Boolean compactedTopic,
                                                      Byte magic,
                                                      TimestampType timestampType,
                                                      Long timestampDiffMaxMs,
                                                      Integer partitionLeaderEpoch,
                                                      Boolean isFromClient): ValidationAndOffsetAssignResult = {
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // check the magic value;
      if (!records.hasMatchingMagic(magic))
        convertAndAssignOffsetsNonCompressed(records, offsetCounter, compactedTopic, now, timestampType,
          timestampDiffMaxMs, magic, partitionLeaderEpoch, isFromClient)
      else;
        // Do in-place validation, offset assignment and maybe set timestamp;
        assignOffsetsNonCompressed(records, offsetCounter, now, compactedTopic, timestampType, timestampDiffMaxMs,
          partitionLeaderEpoch, isFromClient, magic);
    } else {
      validateMessagesAndAssignOffsetsCompressed(records, offsetCounter, now, sourceCodec, targetCodec, compactedTopic,
        magic, timestampType, timestampDiffMaxMs, partitionLeaderEpoch, isFromClient)
    }
  }

  private public void  validateBatch(RecordBatch batch, Boolean isFromClient, Byte toMagic): Unit = {
    if (isFromClient) {
      if (batch.hasProducerId && batch.baseSequence < 0)
        throw new InvalidRecordException(s"Invalid sequence number ${batch.baseSequence} in record batch " +;
          s"with producerId ${batch.producerId}");

      if (batch.isControlBatch)
        throw new InvalidRecordException("Clients are not allowed to write control records");
    }

    if (batch.isTransactional && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Transactional records cannot be used with magic version $toMagic");

    if (batch.hasProducerId && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Idempotent records cannot be used with magic version $toMagic");
  }

  private public void  validateRecord(RecordBatch batch, Record record, Long now, TimestampType timestampType,
                             Long timestampDiffMaxMs, Boolean compactedTopic): Unit = {
    if (!record.hasMagic(batch.magic))
      throw new InvalidRecordException(s"Log record magic does not match outer magic ${batch.magic}");

    // verify the record-level CRC only if this is one of the deep entries of a compressed message;
    // set for magic v0 and v1. For non-compressed messages, there is no inner record for magic v0 and v1,
    // so we depend on the batch-level CRC check in Log.analyzeAndValidateRecords(). For magic v2 and above,
    // there is no record-level CRC to check.;
    if (batch.magic <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed)
      record.ensureValid();

    validateKey(record, compactedTopic);
    validateTimestamp(batch, record, now, timestampType, timestampDiffMaxMs)
  }

  private public void  convertAndAssignOffsetsNonCompressed(MemoryRecords records,
                                                   LongRef offsetCounter,
                                                   Boolean compactedTopic,
                                                   Long now,
                                                   TimestampType timestampType,
                                                   Long timestampDiffMaxMs,
                                                   Byte toMagicValue,
                                                   Integer partitionLeaderEpoch,
                                                   Boolean isFromClient): ValidationAndOffsetAssignResult = {
    val sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.value,
      CompressionType.NONE, records.records);

    val (producerId, producerEpoch, sequence, isTransactional) = {
      val first = records.batches.asScala.head;
      (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional);
    }

    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion);
    val builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, timestampType,
      offsetCounter.value, now, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch);

    for (batch <- records.batches.asScala) {
      validateBatch(batch, isFromClient, toMagicValue);

      for (record <- batch.asScala) {
        validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
        builder.appendWithOffset(offsetCounter.getAndIncrement(), record);
      }
    }

    val convertedRecords = builder.build();
    val info = builder.info;
    ValidationAndOffsetAssignResult(
      validatedRecords = convertedRecords,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true);
  }

  private public void  assignOffsetsNonCompressed(MemoryRecords records,
                                         LongRef offsetCounter,
                                         Long now,
                                         Boolean compactedTopic,
                                         TimestampType timestampType,
                                         Long timestampDiffMaxMs,
                                         Integer partitionLeaderEpoch,
                                         Boolean isFromClient,
                                         Byte magic): ValidationAndOffsetAssignResult = {
    var maxTimestamp = RecordBatch.NO_TIMESTAMP;
    var offsetOfMaxTimestamp = -1L;
    val initialOffset = offsetCounter.value;

    for (batch <- records.batches.asScala) {
      validateBatch(batch, isFromClient, magic);

      var maxBatchTimestamp = RecordBatch.NO_TIMESTAMP;
      var offsetOfMaxBatchTimestamp = -1L;

      for (record <- batch.asScala) {
        validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
        val offset = offsetCounter.getAndIncrement();
        if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && record.timestamp > maxBatchTimestamp) {
          maxBatchTimestamp = record.timestamp;
          offsetOfMaxBatchTimestamp = offset;
        }
      }

      if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
        maxTimestamp = maxBatchTimestamp;
        offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp;
      }

      batch.setLastOffset(offsetCounter.value - 1);

      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch);

      if (batch.magic > RecordBatch.MAGIC_VALUE_V0) {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now);
        else;
          batch.setMaxTimestamp(timestampType, maxBatchTimestamp);
      }
    }

    if (timestampType == TimestampType.LOG_APPEND_TIME) {
      maxTimestamp = now;
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        offsetOfMaxTimestamp = offsetCounter.value - 1;
      else;
        offsetOfMaxTimestamp = initialOffset;
    }

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = maxTimestamp,
      shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp,
      messageSizeMaybeChanged = false);
  }

  /**
   * We cannot do in place assignment in one of the following situations:
   * 1. Source and target compression codec are different
   * 2. When magic value to use is 0 because offsets need to be overwritten
   * 3. When magic value to use is above 0, but some fields of inner messages need to be overwritten.
   * 4. Message format conversion is needed.
   */
  public void  validateMessagesAndAssignOffsetsCompressed(MemoryRecords records,
                                                 LongRef offsetCounter,
                                                 Long now,
                                                 CompressionCodec sourceCodec,
                                                 CompressionCodec targetCodec,
                                                 Boolean compactedTopic,
                                                 Byte toMagic,
                                                 TimestampType timestampType,
                                                 Long timestampDiffMaxMs,
                                                 Integer partitionLeaderEpoch,
                                                 Boolean isFromClient): ValidationAndOffsetAssignResult = {

      // No in place assignment situation 1 and 2;
      var inPlaceAssignment = sourceCodec == targetCodec && toMagic > RecordBatch.MAGIC_VALUE_V0;

      var maxTimestamp = RecordBatch.NO_TIMESTAMP;
      val expectedInnerOffset = new LongRef(0);
      val validatedRecords = new mutable.ArrayBuffer<Record>;

      for (batch <- records.batches.asScala) {
        validateBatch(batch, isFromClient, toMagic);

        // Do not compress control records unless they are written compressed;
        if (sourceCodec == NoCompressionCodec && batch.isControlBatch)
          inPlaceAssignment = true;

        for (record <- batch.asScala) {
          validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic)
          if (sourceCodec != NoCompressionCodec && record.isCompressed)
            throw new InvalidRecordException("Compressed outer record should not have an inner record with a " +;
              s"compression attribute set: $record");
          if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && toMagic > RecordBatch.MAGIC_VALUE_V0) {
            // Check if we need to overwrite offset;
            // No in place assignment situation 3;
            if (record.offset != expectedInnerOffset.getAndIncrement())
              inPlaceAssignment = false;
            if (record.timestamp > maxTimestamp)
              maxTimestamp = record.timestamp;
          }

          // No in place assignment situation 4;
          if (!record.hasMagic(toMagic))
            inPlaceAssignment = false;

          validatedRecords += record;
        }
      }

      if (!inPlaceAssignment) {
        val (producerId, producerEpoch, sequence, isTransactional) = {
          // note that we only reassign offsets for requests coming straight from a producer. For records with magic V2,
          // there should be exactly one RecordBatch per request, so the following is all we need to do. For Records;
          // with older magic versions, there will never be a producer id, etc.;
          val first = records.batches.asScala.head;
          (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional);
        }
        buildRecordsAndAssignOffsets(toMagic, offsetCounter, timestampType, CompressionType.forId(targetCodec.codec), now,
          validatedRecords, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch);
      } else {
        // we can update the batch only and write the compressed payload as is;
        val batch = records.batches.iterator.next();
        val lastOffset = offsetCounter.addAndGet(validatedRecords.size) - 1;

        batch.setLastOffset(lastOffset);

        if (timestampType == TimestampType.LOG_APPEND_TIME)
          maxTimestamp = now;

        if (toMagic >= RecordBatch.MAGIC_VALUE_V1)
          batch.setMaxTimestamp(timestampType, maxTimestamp);

        if (toMagic >= RecordBatch.MAGIC_VALUE_V2)
          batch.setPartitionLeaderEpoch(partitionLeaderEpoch);

        ValidationAndOffsetAssignResult(validatedRecords = records,
          maxTimestamp = maxTimestamp,
          shallowOffsetOfMaxTimestamp = lastOffset,
          messageSizeMaybeChanged = false);
      }
  }

  private public void  buildRecordsAndAssignOffsets(Byte magic,
                                           LongRef offsetCounter,
                                           TimestampType timestampType,
                                           CompressionType compressionType,
                                           Long logAppendTime,
                                           Seq validatedRecords<Record>,
                                           Long producerId,
                                           Short producerEpoch,
                                           Integer baseSequence,
                                           Boolean isTransactional,
                                           Integer partitionLeaderEpoch): ValidationAndOffsetAssignResult = {
    val estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.value, compressionType,
      validatedRecords.asJava);
    val buffer = ByteBuffer.allocate(estimatedSize);
    val builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType, offsetCounter.value,
      logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch);

    validatedRecords.foreach { record =>
      builder.appendWithOffset(offsetCounter.getAndIncrement(), record);
    }

    val records = builder.build();
    val info = builder.info;

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true);
  }

  private public void  validateKey(Record record, Boolean compactedTopic) {
    if (compactedTopic && !record.hasKey)
      throw new InvalidRecordException("Compacted topic cannot accept message without key.");
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is within acceptable range.
   */
  private public void  validateTimestamp(RecordBatch batch,
                                Record record,
                                Long now,
                                TimestampType timestampType,
                                Long timestampDiffMaxMs) {
    if (timestampType == TimestampType.CREATE_TIME;
      && record.timestamp != RecordBatch.NO_TIMESTAMP;
      && math.abs(record.timestamp - now) > timestampDiffMaxMs)
      throw new InvalidTimestampException(s"Timestamp ${record.timestamp} of message with offset ${record.offset} is " +;
        s"out of range. The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}]")
    if (batch.timestampType == TimestampType.LOG_APPEND_TIME)
      throw new InvalidTimestampException(s"Invalid timestamp type in message $record. Producer should not set " +;
        s"timestamp type to LogAppendTime.");
  }

  case class ValidationAndOffsetAssignResult(MemoryRecords validatedRecords,
                                             Long maxTimestamp,
                                             Long shallowOffsetOfMaxTimestamp,
                                             Boolean messageSizeMaybeChanged);

}
