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

import java.io.File;
import java.nio._;
import java.util.Date;
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Gauge;
import kafka.common._;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils._;
import org.apache.kafka.common.record._;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords.RecordFilter;

import scala.collection.mutable;
import scala.collection.JavaConverters._;

/**
 * The cleaner is responsible for removing obsolete records from logs which have the "compact" retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * 
 * Each log can be thought of being split into two sections of a segments "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The dirty section is further divided into the "cleanable" section followed by an "uncleanable" section.
 * The uncleanable section is excluded from cleaning. The active log segment is always uncleanable. If there is a
 * compaction lag time set, segments whose largest message timestamp is within the compaction lag time of the cleaning operation are also uncleanable.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "compact" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log. 
 * 
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping. 
 * 
 * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a 
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 * 
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 * 
 * Cleaned segments are swapped into the log as they become available.
 * 
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 * 
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner. 
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 * 
 * @param config Configuration parameters for the cleaner
 * @param logDirs The directories where offset checkpoints reside
 * @param logs The pool of logs
 * @param time A way to control the passage of time
 */
class LogCleaner(val CleanerConfig config,
                 val Array logDirs<File>,
                 val Pool logs<TopicPartition, Log>,
                 Time time = Time.SYSTEM) extends Logging with KafkaMetricsGroup {
  ;
  /* for managing the state of partitions being cleaned. package-private to allow access in tests */
  private<log> val cleanerManager = new LogCleanerManager(logDirs, logs);

  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond, ;
                                        checkIntervalMs = 300, ;
                                        throttleDown = true, ;
                                        "cleaner-io",
                                        "bytes",
                                        time = time);
  ;
  /* the threads */
  private val cleaners = (0 until config.numThreads).map(new CleanerThread(_));
  ;
  /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
  newGauge("max-buffer-utilization-percent", ;
           new Gauge<Int> {
             public void  Integer value = cleaners.map(_.lastStats).map(100 * _.bufferUtilization).max.toInt;
           });
  /* a metric to track the recopy rate of each thread's last cleaning */
  newGauge("cleaner-recopy-percent", ;
           new Gauge<Int> {
             public void  Integer value = {
               val stats = cleaners.map(_.lastStats);
               val recopyRate = stats.map(_.bytesWritten).sum.toDouble / math.max(stats.map(_.bytesRead).sum, 1);
               (100 * recopyRate).toInt;
             }
           });
  /* a metric to track the maximum cleaning time for the last cleaning from each thread */
  newGauge("max-clean-time-secs",
           new Gauge<Int> {
             public void  Integer value = cleaners.map(_.lastStats).map(_.elapsedSecs).max.toInt;
           });
  ;
  /**
   * Start the background cleaning
   */
  public void  startup() {
    info("Starting the log cleaner");
    cleaners.foreach(_.start())
  }
  ;
  /**
   * Stop the background cleaning
   */
  public void  shutdown() {
    info("Shutting down the log cleaner.");
    cleaners.foreach(_.shutdown())
  }
  ;
  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   */
  public void  abortCleaning(TopicPartition topicPartition) {
    cleanerManager.abortCleaning(topicPartition);
  }

  /**
   * Update checkpoint file, removing topics and partitions that no longer exist
   */
  public void  updateCheckpoints(File dataDir) {
    cleanerManager.updateCheckpoints(dataDir, update=None);
  }

  /**
   * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
   */
  public void  maybeTruncateCheckpoint(File dataDir, TopicPartition topicPartition, Long offset) {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset);
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   */
  public void  abortAndPauseCleaning(TopicPartition topicPartition) {
    cleanerManager.abortAndPauseCleaning(topicPartition);
  }

  /**
   *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
   */
  public void  resumeCleaning(TopicPartition topicPartition) {
    cleanerManager.resumeCleaning(topicPartition);
  }

  /**
   * For testing, a way to know when work has completed. This method waits until the
   * cleaner has processed up to the given offset on the specified topic/partition
   *
   * @param topicPartition The topic and partition to be cleaned
   * @param offset The first dirty offset that the cleaner doesn't have to clean
   * @param maxWaitMs The maximum time in ms to wait for cleaner
   *
   * @return A boolean indicating whether the work has completed before timeout
   */
  public void  awaitCleaned(TopicPartition topicPartition, Long offset, Long maxWaitMs = 60000L): Boolean = {
    public void  isCleaned = cleanerManager.allCleanerCheckpoints.get(topicPartition).fold(false)(_ >= offset);
    var remainingWaitMs = maxWaitMs;
    while (!isCleaned && remainingWaitMs > 0) {
      val sleepTime = math.min(100, remainingWaitMs);
      Thread.sleep(sleepTime);
      remainingWaitMs -= sleepTime;
    }
    isCleaned;
  }
  ;
  /**
   * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
   * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
   */
  private class CleanerThread Integer threadId);
    extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId, isInterruptible = false) {
    ;
    override val loggerName = classOf<LogCleaner>.getName;
    ;
    if(config.dedupeBufferSize / config.numThreads > Int.MaxValue)
      warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...");

    val cleaner = new Cleaner(id = threadId,
                              offsetMap = new SkimpyOffsetMap(memory = math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt, ;
                                                              hashAlgorithm = config.hashAlgorithm),
                              ioBufferSize = config.ioBufferSize / config.numThreads / 2,
                              maxIoBufferSize = config.maxMessageSize,
                              dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                              throttler = throttler,
                              time = time,
                              checkDone = checkDone);
    ;
    @volatile var CleanerStats lastStats = new CleanerStats()
    private val backOffWaitLatch = new CountDownLatch(1);

    private public void  checkDone(TopicPartition topicPartition) {
      if (!isRunning.get())
        throw new ThreadShutdownException;
      cleanerManager.checkCleaningAborted(topicPartition);
    }

    /**
     * The main loop for the cleaner thread
     */
    override public void  doWork() {
      cleanOrSleep();
    }

    override public void  shutdown() = {
    	 initiateShutdown();
    	 backOffWaitLatch.countDown();
    	 awaitShutdown();
     }
     ;
    /**
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
     */
    private public void  cleanOrSleep() {
      val cleaned = cleanerManager.grabFilthiestCompactedLog(time) match {
        case None =>
          false;
        case Some(cleanable) =>
          // there's a log, clean it;
          var endOffset = cleanable.firstDirtyOffset;
          try {
            val (nextDirtyOffset, cleanerStats) = cleaner.clean(cleanable);
            recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleanerStats);
            endOffset = nextDirtyOffset;
          } catch {
            case LogCleaningAbortedException _ => // task can be aborted, let it go.;
          } finally {
            cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset);
          }
          true;
      }
      val Iterable deletable<(TopicPartition, Log)> = cleanerManager.deletableLogs();
      deletable.foreach{
        case (topicPartition, log) =>
          try {
            log.deleteOldSegments();
          } finally {
            cleanerManager.doneDeleting(topicPartition);
          }
      }
      if (!cleaned)
        backOffWaitLatch.await(config.backOffMs, TimeUnit.MILLISECONDS);
    }
    ;
    /**
     * Log out statistics on a single run of the cleaner.
     */
    public void  recordStats Integer id, String name, Long from, Long to, CleanerStats stats) {
      this.lastStats = stats;
      public void  mb(Double bytes) = bytes / (1024*1024);
      val message = ;
        String.format("%n\tLog cleaner thread %d cleaned log %s (dirty section = <%d, %d>)%n",id, name, from, to) + ;
        String.format("\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n",mb(stats.bytesRead), ;
                                                                                stats.elapsedSecs, ;
                                                                                mb(stats.bytesRead/stats.elapsedSecs)) + ;
        String.format("\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n",mb(stats.mapBytesRead), ;
                                                                                           stats.elapsedIndexSecs, ;
                                                                                           mb(stats.mapBytesRead)/stats.elapsedIndexSecs, ;
                                                                                           100 * stats.elapsedIndexSecs/stats.elapsedSecs) +;
        String.format("\tBuffer utilization: %.1f%%%n",100 * stats.bufferUtilization) +;
        String.format("\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n",mb(stats.bytesRead), ;
                                                                                           stats.elapsedSecs - stats.elapsedIndexSecs, ;
                                                                                           mb(stats.bytesRead)/(stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble/stats.elapsedSecs) + ;
        String.format("\tStart size: %,.1f MB (%,d messages)%n",mb(stats.bytesRead), stats.messagesRead) +;
        String.format("\tEnd size: %,.1f MB (%,d messages)%n",mb(stats.bytesWritten), stats.messagesWritten) + ;
        String.format("\t%.1f%% size reduction (%.1f%% fewer messages)%n",100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead), ;
                                                                   100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead));
      info(message);
      if (stats.invalidMessagesRead > 0) {
        warn(String.format("\tFound %d invalid messages during compaction.",stats.invalidMessagesRead))
      }
    }
   ;
  }
}

/**
 * This class holds the actual logic for cleaning a log
 * @param id An identifier used for logging
 * @param offsetMap The map used for deduplication
 * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
 * @param maxIoBufferSize The maximum size of a message that can appear in the log
 * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param throttler The throttler instance to use for limiting I/O rate.
 * @param time The time instance
 * @param checkDone Check if the cleaning for a partition is finished or aborted.
 */
private<log> class Cleaner(val Integer id,
                           val OffsetMap offsetMap,
                           Integer ioBufferSize,
                           Integer maxIoBufferSize,
                           Double dupBufferLoadFactor,
                           Throttler throttler,
                           Time time,
                           checkDone: (TopicPartition) => Unit) extends Logging {
  ;
  override val loggerName = classOf<LogCleaner>.getName;

  this.logIdent = "Cleaner " + id + ": ";

  /* buffer used for read i/o */
  private var readBuffer = ByteBuffer.allocate(ioBufferSize);
  ;
  /* buffer used for write i/o */
  private var writeBuffer = ByteBuffer.allocate(ioBufferSize);

  private val decompressionBufferSupplier = BufferSupplier.create();

  require(offsetMap.slots * dupBufferLoadFactor > 1, "offset map is too small to fit in even a single message, so log cleaning will never make progress. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads");

  /**
   * Clean the given log
   *
   * @param cleanable The log to be cleaned
   *
   * @return The first offset not cleaned and the statistics for this round of cleaning
   */
  private<log> public void  clean(LogToClean cleanable): (Long, CleanerStats) = {
    // figure out the timestamp below which it is safe to remove delete tombstones;
    // this position is defined to be a configurable time beneath the last modified time of the last clean segment;
    val deleteHorizonMs = ;
      cleanable.log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L;
        case Some(seg) => seg.lastModified - cleanable.log.config.deleteRetentionMs;
    }

    doClean(cleanable, deleteHorizonMs);
  }

  private<log> public void  doClean(LogToClean cleanable, Long deleteHorizonMs): (Long, CleanerStats) = {
    info(String.format("Beginning cleaning of log %s.",cleanable.log.name))

    val log = cleanable.log;
    val stats = new CleanerStats();

    // build the offset map;
    info(String.format("Building offset map for %s...",cleanable.log.name))
    val upperBoundOffset = cleanable.firstUncleanableOffset;
    buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap, stats);
    val endOffset = offsetMap.latestOffset + 1;
    stats.indexDone();

    // determine the timestamp up to which the log will be cleaned;
    // this is the lower of the last active segment and the compaction lag;
    val cleanableHorizonMs = log.logSegments(0, cleanable.firstUncleanableOffset).lastOption.map(_.lastModified).getOrElse(0L)


    // group the segments and clean the groups;
    info(String.format("Cleaning log %s (cleaning prior to %s, discarding tombstones prior to %s)...",log.name, new Date(cleanableHorizonMs), new Date(deleteHorizonMs)))
    for (group <- groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize, cleanable.firstUncleanableOffset))
      cleanSegments(log, group, offsetMap, deleteHorizonMs, stats);

    // record buffer utilization;
    stats.bufferUtilization = offsetMap.utilization;

    stats.allDone();

    (endOffset, stats);
  }

  /**
   * Clean a group of segments into a single replacement segment
   *
   * @param log The log being cleaned
   * @param segments The group of segments being cleaned
   * @param map The offset map to use for cleaning segments
   * @param deleteHorizonMs The time to retain delete tombstones
   * @param stats Collector for cleaning statistics
   */
  private<log> public void  cleanSegments(Log log,
                                 Seq segments<LogSegment>,
                                 OffsetMap map,
                                 Long deleteHorizonMs,
                                 CleanerStats stats) {
    // create a new segment with the suffix .cleaned appended to both the log and index name;
    val logFile = new File(segments.head.log.file.getPath + Log.CleanedFileSuffix);
    logFile.delete();
    val indexFile = new File(segments.head.index.file.getPath + Log.CleanedFileSuffix);
    val timeIndexFile = new File(segments.head.timeIndex.file.getPath + Log.CleanedFileSuffix);
    val txnIndexFile = new File(segments.head.txnIndex.file.getPath + Log.CleanedFileSuffix);
    indexFile.delete();
    timeIndexFile.delete();
    txnIndexFile.delete();

    val startOffset = segments.head.baseOffset;
    val records = FileRecords.open(logFile, false, log.initFileSize(), log.config.preallocate);
    val index = new OffsetIndex(indexFile, startOffset, segments.head.index.maxIndexSize);
    val timeIndex = new TimeIndex(timeIndexFile, startOffset, segments.head.timeIndex.maxIndexSize);
    val txnIndex = new TransactionIndex(startOffset, txnIndexFile);
    val cleaned = new LogSegment(records, index, timeIndex, txnIndex, startOffset,
      segments.head.indexIntervalBytes, log.config.randomSegmentJitter, time);

    try {
      // clean segments into the new destination segment;
      val iter = segments.iterator;
      var Option currentSegmentOpt<LogSegment> = Some(iter.next());
      while (currentSegmentOpt.isDefined) {
        val oldSegmentOpt = currentSegmentOpt.get;
        val nextSegmentOpt = if (iter.hasNext) Some(iter.next()) else None;

        val startOffset = oldSegmentOpt.baseOffset;
        val upperBoundOffset = nextSegmentOpt.map(_.baseOffset).getOrElse(map.latestOffset + 1);
        val abortedTransactions = log.collectAbortedTransactions(startOffset, upperBoundOffset);
        val transactionMetadata = CleanedTransactionMetadata(abortedTransactions, Some(txnIndex));

        val retainDeletes = oldSegmentOpt.lastModified > deleteHorizonMs;
        info("Cleaning segment %s in log %s (largest timestamp %s) into %s, %s deletes.";
          .format(startOffset, log.name, new Date(oldSegmentOpt.largestTimestamp), cleaned.baseOffset, if(retainDeletes) "retaining" else "discarding"))
        cleanInto(log.topicPartition, oldSegmentOpt, cleaned, map, retainDeletes, log.config.maxMessageSize, transactionMetadata,
          log.activeProducers, stats);

        currentSegmentOpt = nextSegmentOpt;
      }

      // trim excess index;
      index.trimToValidSize();

      // Append the last index entry;
      cleaned.onBecomeInactiveSegment();

      // trim time index;
      timeIndex.trimToValidSize();

      // flush new segment to disk before swap;
      cleaned.flush();

      // update the modification date to retain the last modified date of the original files;
      val modified = segments.last.lastModified;
      cleaned.lastModified = modified;

      // swap in new segment;
      info(String.format("Swapping in cleaned segment %d for segment(s) %s in log %s.",cleaned.baseOffset, segments.map(_.baseOffset).mkString(","), log.name))
      log.replaceSegments(cleaned, segments);
    } catch {
      case LogCleaningAbortedException e =>
        cleaned.delete();
        throw e;
    }
  }

  /**
   * Clean the given source log segment into the destination segment using the key=>offset mapping
   * provided
   *
   * @param topicPartition The topic and partition of the log segment to clean
   * @param source The dirty log segment
   * @param dest The cleaned log segment
   * @param map The key=>offset mapping
   * @param retainDeletes Should delete tombstones be retained while cleaning this segment
   * @param maxLogMessageSize The maximum message size of the corresponding topic
   * @param stats Collector for cleaning statistics
   */
  private<log> public void  cleanInto(TopicPartition topicPartition,
                             LogSegment source,
                             LogSegment dest,
                             OffsetMap map,
                             Boolean retainDeletes,
                             Integer maxLogMessageSize,
                             CleanedTransactionMetadata transactionMetadata,
                             Map activeProducers<Long, ProducerIdEntry>,
                             CleanerStats stats) {
    val logCleanerFilter = new RecordFilter {
      var Boolean retainLastBatchSequence = false;
      var Boolean discardBatchRecords = false;

      override public void  shouldDiscard(RecordBatch batch): Boolean = {
        // we piggy-back on the tombstone retention logic to delay deletion of transaction markers.;
        // note that we will never delete a marker until all the records from that transaction are removed.;
        discardBatchRecords = shouldDiscardBatch(batch, transactionMetadata, retainTxnMarkers = retainDeletes);

        // check if the batch contains the last sequence number for the producer. if so, we cannot;
        // remove the batch just yet or the producer may see an out of sequence error.;
        if (batch.hasProducerId && activeProducers.get(batch.producerId).exists(_.lastSeq == batch.lastSequence)) {
          retainLastBatchSequence = true;
          false;
        } else {
          retainLastBatchSequence = false;
          discardBatchRecords;
        }
      }

      override public void  shouldRetain(RecordBatch batch, Record record): Boolean = {
        if (retainLastBatchSequence && batch.lastSequence == record.sequence)
          // always retain the record with the last sequence number;
          true;
        else if (discardBatchRecords)
          // remove the record if the batch would have otherwise been discarded;
          false;
        else;
          shouldRetainRecord(source, map, retainDeletes, batch, record, stats);
      }
    }

    var position = 0;
    while (position < source.log.sizeInBytes) {
      checkDone(topicPartition);
      // read a chunk of messages and copy any that are to be retained to the write buffer to be written out;
      readBuffer.clear();
      writeBuffer.clear();

      source.log.readInto(readBuffer, position);
      val records = MemoryRecords.readableRecords(readBuffer);
      throttler.maybeThrottle(records.sizeInBytes);
      val result = records.filterTo(topicPartition, logCleanerFilter, writeBuffer, maxLogMessageSize, decompressionBufferSupplier);
      stats.readMessages(result.messagesRead, result.bytesRead);
      stats.recopyMessages(result.messagesRetained, result.bytesRetained);

      position += result.bytesRead;

      // if any messages are to be retained, write them out;
      val outputBuffer = result.output;
      if (outputBuffer.position > 0) {
        outputBuffer.flip();
        val retained = MemoryRecords.readableRecords(outputBuffer);
        dest.append(firstOffset = retained.batches.iterator.next().baseOffset,
          largestOffset = result.maxOffset,
          largestTimestamp = result.maxTimestamp,
          shallowOffsetOfMaxTimestamp = result.shallowOffsetOfMaxTimestamp,
          records = retained);
        throttler.maybeThrottle(outputBuffer.limit);
      }

      // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again;
      if (readBuffer.limit > 0 && result.messagesRead == 0)
        growBuffers(maxLogMessageSize);
    }
    restoreBuffers();
  }

  private public void  shouldDiscardBatch(RecordBatch batch,
                                 CleanedTransactionMetadata transactionMetadata,
                                 Boolean retainTxnMarkers): Boolean = {
    if (batch.isControlBatch) {
      val canDiscardControlBatch = transactionMetadata.onControlBatchRead(batch);
      canDiscardControlBatch && !retainTxnMarkers;
    } else {
      val canDiscardBatch = transactionMetadata.onBatchRead(batch);
      canDiscardBatch;
    }
  }

  private public void  shouldRetainRecord(kafka source.log.LogSegment,
                                 kafka map.log.OffsetMap,
                                 Boolean retainDeletes,
                                 RecordBatch batch,
                                 Record record,
                                 CleanerStats stats): Boolean = {
    val pastLatestOffset = record.offset > map.latestOffset;
    if (pastLatestOffset)
      return true;

    if (record.hasKey) {
      val key = record.key;
      val foundOffset = map.get(key);
      /* two cases in which we can get rid of a message:
       *   1) if there exists a message with the same key but higher offset
       *   2) if the message is a delete "tombstone" marker and enough time has passed
       */
      val redundant = foundOffset >= 0 && record.offset < foundOffset;
      val obsoleteDelete = !retainDeletes && !record.hasValue;
      !redundant && !obsoleteDelete;
    } else {
      stats.invalidMessage();
      false;
    }
  }

  /**
   * Double the I/O buffer capacity
   */
  public void  growBuffers Integer maxLogMessageSize) {
    val maxBufferSize = math.max(maxLogMessageSize, maxIoBufferSize);
    if(readBuffer.capacity >= maxBufferSize || writeBuffer.capacity >= maxBufferSize)
      throw new IllegalStateException(String.format("This log contains a message larger than maximum allowable size of %s.",maxBufferSize))
    val newSize = math.min(this.readBuffer.capacity * 2, maxBufferSize);
    info("Growing cleaner I/O buffers from " + readBuffer.capacity + "bytes to " + newSize + " bytes.");
    this.readBuffer = ByteBuffer.allocate(newSize);
    this.writeBuffer = ByteBuffer.allocate(newSize);
  }

  /**
   * Restore the I/O buffer capacity to its original size
   */
  public void  restoreBuffers() {
    if(this.readBuffer.capacity > this.ioBufferSize)
      this.readBuffer = ByteBuffer.allocate(this.ioBufferSize);
    if(this.writeBuffer.capacity > this.ioBufferSize)
      this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize);
  }

  /**
   * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
   * We collect a group of such segments together into a single
   * destination segment. This prevents segment sizes from shrinking too much.
   *
   * @param segments The log segments to group
   * @param maxSize the maximum size in bytes for the total of all log data in a group
   * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
   *
   * @return A list of grouped segments
   */
  private<log> public void  groupSegmentsBySize(Iterable segments<LogSegment>, Integer maxSize, Integer maxIndexSize, Long firstUncleanableOffset): List<Seq[LogSegment]> = {
    var grouped = List<List[LogSegment]>();
    var segs = segments.toList;
    while(segs.nonEmpty) {
      var group = List(segs.head);
      var logSize = segs.head.size;
      var indexSize = segs.head.index.sizeInBytes;
      var timeIndexSize = segs.head.timeIndex.sizeInBytes;
      segs = segs.tail;
      while(segs.nonEmpty &&;
            logSize + segs.head.size <= maxSize &&;
            indexSize + segs.head.index.sizeInBytes <= maxIndexSize &&;
            timeIndexSize + segs.head.timeIndex.sizeInBytes <= maxIndexSize &&;
            lastOffsetForFirstSegment(segs, firstUncleanableOffset) - group.last.baseOffset <= Int.MaxValue) {
        group = segs.head :: group;
        logSize += segs.head.size;
        indexSize += segs.head.index.sizeInBytes;
        timeIndexSize += segs.head.timeIndex.sizeInBytes;
        segs = segs.tail;
      }
      grouped ::= group.reverse;
    }
    grouped.reverse;
  }

  /**
    * We want to get the last offset in the first log segment in segs.
    * LogSegment.nextOffset() gives the exact last offset in a segment, but can be expensive since it requires
    * scanning the segment from the last index entry.
    * Therefore, we estimate the last offset of the first log segment by using
    * the base offset of the next segment in the list.
    * If the next segment doesn't exist, first Uncleanable Offset will be used.
    *
    * @param segs - remaining segments to group.
    * @return The estimated last offset for the first segment in segs
    */
  private public void  lastOffsetForFirstSegment(List segs<LogSegment>, Long firstUncleanableOffset): Long = {
    if (segs.size > 1) {
      /* if there is a next segment, use its base offset as the bounding offset to guarantee we know
       * the worst case offset */
      segs(1).baseOffset - 1;
    } else {
      //for the last segment in the list, use the first uncleanable offset.;
      firstUncleanableOffset - 1;
    }
  }

  /**
   * Build a map of key_hash => offset for the keys in the cleanable dirty portion of the log to use in cleaning.
   * @param log The log to use
   * @param start The offset at which dirty messages begin
   * @param end The ending offset for the map that is being built
   * @param map The map in which to store the mappings
   * @param stats Collector for cleaning statistics
   */
  private<log> public void  buildOffsetMap(Log log,
                                  Long start,
                                  Long end,
                                  OffsetMap map,
                                  CleanerStats stats) {
    map.clear();
    val dirty = log.logSegments(start, end).toBuffer;
    info(String.format("Building offset map for log %s for %d segments in offset range [%d, %d).",log.name, dirty.size, start, end))

    val abortedTransactions = log.collectAbortedTransactions(start, end);
    val transactionMetadata = CleanedTransactionMetadata(abortedTransactions);

    // Add all the cleanable dirty segments. We must take at least map.slots * load_factor,
    // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
    var full = false;
    for (segment <- dirty if !full) {
      checkDone(log.topicPartition);

      full = buildOffsetMapForSegment(log.topicPartition, segment, map, start, log.config.maxMessageSize,
        transactionMetadata, stats);
      if (full)
        debug(String.format("Offset map is full, %d segments fully mapped, segment with base offset %d is partially mapped",dirty.indexOf(segment), segment.baseOffset))
    }
    info(String.format("Offset map for log %s complete.",log.name))
  }

  /**
   * Add the messages in the given segment to the offset map
   *
   * @param segment The segment to index
   * @param map The map in which to store the key=>offset mapping
   * @param stats Collector for cleaning statistics
   *
   * @return If the map was filled whilst loading from this segment
   */
  private public void  buildOffsetMapForSegment(TopicPartition topicPartition,
                                       LogSegment segment,
                                       OffsetMap map,
                                       Long startOffset,
                                       Integer maxLogMessageSize,
                                       CleanedTransactionMetadata transactionMetadata,
                                       CleanerStats stats): Boolean = {
    var position = segment.index.lookup(startOffset).position;
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt;
    while (position < segment.log.sizeInBytes) {
      checkDone(topicPartition);
      readBuffer.clear();
      segment.log.readInto(readBuffer, position);
      val records = MemoryRecords.readableRecords(readBuffer);
      throttler.maybeThrottle(records.sizeInBytes);

      val startPosition = position;
      for (batch <- records.batches.asScala) {
        if (batch.isControlBatch) {
          transactionMetadata.onControlBatchRead(batch);
          stats.indexMessagesRead(1);
        } else {
          val isAborted = transactionMetadata.onBatchRead(batch);
          if (isAborted) {
            // abort markers are supported in v2 and above, which means count is defined;
            stats.indexMessagesRead(batch.countOrNull);
          } else {
            for (record <- batch.asScala) {
              if (record.hasKey && record.offset >= startOffset) {
                if (map.size < maxDesiredMapSize)
                  map.put(record.key, record.offset);
                else;
                  return true;
              }
              stats.indexMessagesRead(1);
            }
          }
        }

        if (batch.lastOffset >= startOffset)
          map.updateLatestOffset(batch.lastOffset);
      }
      val bytesRead = records.validBytes;
      position += bytesRead;
      stats.indexBytesRead(bytesRead);

      // if we didn't read even one complete message, our read buffer may be too small;
      if(position == startPosition)
        growBuffers(maxLogMessageSize);
    }
    restoreBuffers();
    false;
  }
}

/**
 * A simple struct for collecting stats about log cleaning
 */
private class CleanerStats(Time time = Time.SYSTEM) {
  val startTime = time.milliseconds;
  var mapCompleteTime = -1L;
  var endTime = -1L;
  var bytesRead = 0L;
  var bytesWritten = 0L;
  var mapBytesRead = 0L;
  var mapMessagesRead = 0L;
  var messagesRead = 0L;
  var invalidMessagesRead = 0L;
  var messagesWritten = 0L;
  var bufferUtilization = 0.0d;

  public void  readMessages Integer messagesRead, Integer bytesRead) {
    this.messagesRead += messagesRead;
    this.bytesRead += bytesRead;
  }

  public void  invalidMessage() {
    invalidMessagesRead += 1;
  }

  public void  recopyMessages Integer messagesWritten, Integer bytesWritten) {
    this.messagesWritten += messagesWritten;
    this.bytesWritten += bytesWritten;
  }

  public void  indexMessagesRead Integer size) {
    mapMessagesRead += size;
  }

  public void  indexBytesRead Integer size) {
    mapBytesRead += size;
  }

  public void  indexDone() {
    mapCompleteTime = time.milliseconds;
  }

  public void  allDone() {
    endTime = time.milliseconds;
  }

  public void  elapsedSecs = (endTime - startTime)/1000.0;

  public void  elapsedIndexSecs = (mapCompleteTime - startTime)/1000.0;

}

/**
 * Helper class for a log, its topic/partition, the first cleanable position, and the first uncleanable dirty position
 */
private case class LogToClean(TopicPartition topicPartition, Log log, Long firstDirtyOffset, Long uncleanableOffset) extends Ordered<LogToClean> {
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size).sum;
  private<this> val firstUncleanableSegment = log.logSegments(uncleanableOffset, log.activeSegment.baseOffset).headOption.getOrElse(log.activeSegment);
  val firstUncleanableOffset = firstUncleanableSegment.baseOffset;
  val cleanableBytes = log.logSegments(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableOffset)).map(_.size).sum;
  val totalBytes = cleanBytes + cleanableBytes;
  val cleanableRatio = cleanableBytes / totalBytes.toDouble;
  override public void  compare(LogToClean that): Integer = math.signum(this.cleanableRatio - that.cleanableRatio).toInt;
}

private<log> object CleanedTransactionMetadata {
  public void  apply(List abortedTransactions<AbortedTxn>,
            Option transactionIndex<TransactionIndex> = None): CleanedTransactionMetadata = {
    val queue = mutable.PriorityQueue.empty<AbortedTxn](new Ordering[AbortedTxn> {
      override public void  compare(AbortedTxn x, AbortedTxn y): Integer = x.firstOffset compare y.firstOffset;
    }.reverse);
    queue ++= abortedTransactions;
    new CleanedTransactionMetadata(queue, transactionIndex);
  }

  val Empty = CleanedTransactionMetadata(List.empty<AbortedTxn>);
}

/**
 * This is a helper class to facilitate tracking transaction state while cleaning the log. It is initialized
 * with the aborted transactions from the transaction index and its state is updated as the cleaner iterates through
 * the log during a round of cleaning. This class is responsible for deciding when transaction markers can
 * be removed and is therefore also responsible for updating the cleaned transaction index accordingly.
 */
private<log> class CleanedTransactionMetadata(val mutable abortedTransactions.PriorityQueue<AbortedTxn>,
                                              val Option transactionIndex<TransactionIndex> = None) {
  val ongoingCommittedTxns = mutable.Set.empty<Long>;
  val ongoingAbortedTxns = mutable.Map.empty<Long, AbortedTxn>;

  /**
   * Update the cleaned transaction state with a control batch that has just been traversed by the cleaner.
   * Return true if the control batch can be discarded.
   */
  public void  onControlBatchRead(RecordBatch controlBatch): Boolean = {
    consumeAbortedTxnsUpTo(controlBatch.lastOffset);

    val controlRecord = controlBatch.iterator.next();
    val controlType = ControlRecordType.parse(controlRecord.key);
    val producerId = controlBatch.producerId;
    controlType match {
      case ControlRecordType.ABORT =>
        val maybeAbortedTxn = ongoingAbortedTxns.remove(producerId);
        maybeAbortedTxn.foreach { abortedTxn =>
          transactionIndex.foreach(_.append(abortedTxn))
        }
        true;

      case ControlRecordType.COMMIT =>
        // this marker is eligible for deletion if we didn't traverse any records from the transaction;
        !ongoingCommittedTxns.remove(producerId);

      case _ => false;
    }
  }

  private public void  consumeAbortedTxnsUpTo(Long offset): Unit = {
    while (abortedTransactions.headOption.exists(_.firstOffset <= offset)) {
      val abortedTxn = abortedTransactions.dequeue();
      ongoingAbortedTxns += abortedTxn.producerId -> abortedTxn;
    }
  }

  /**
   * Update the transactional state for the incoming non-control batch. If the batch is part of
   * an aborted transaction, return true to indicate that it is safe to discard.
   */
  public void  onBatchRead(RecordBatch batch): Boolean = {
    consumeAbortedTxnsUpTo(batch.lastOffset);
    if (batch.isTransactional) {
      if (ongoingAbortedTxns.contains(batch.producerId))
        true;
      else {
        ongoingCommittedTxns += batch.producerId;
        false;
      }
    } else {
      false;
    }
  }

}
