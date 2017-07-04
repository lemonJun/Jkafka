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
package kafka.coordinator.transaction;

import kafka.utils.{Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

import scala.collection.{immutable, mutable}

private<transaction> sealed trait TransactionState { public void  Byte byte }

/**
 * Transaction has not existed yet
 *
 * received transition AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private<transaction> case object Empty extends TransactionState { val Byte byte = 0 }

/**
 * Transaction has started and ongoing
 *
 * received transition EndTxnRequest with commit => PrepareCommit
 *             received EndTxnRequest with abort => PrepareAbort
 *             received AddPartitionsToTxnRequest => Ongoing
 *             received AddOffsetsToTxnRequest => Ongoing
 */
private<transaction> case object Ongoing extends TransactionState { val Byte byte = 1 }

/**
 * Group is preparing to commit
 *
 * received transition acks from all partitions => CompleteCommit
 */
private<transaction> case object PrepareCommit extends TransactionState { val Byte byte = 2}

/**
 * Group is preparing to abort
 *
 * received transition acks from all partitions => CompleteAbort
 */
private<transaction> case object PrepareAbort extends TransactionState { val Byte byte = 3 }

/**
 * Group has completed commit
 *
 * Will soon be removed from the ongoing transaction cache
 */
private<transaction> case object CompleteCommit extends TransactionState { val Byte byte = 4 }

/**
 * Group has completed abort
 *
 * Will soon be removed from the ongoing transaction cache
 */
private<transaction> case object CompleteAbort extends TransactionState { val Byte byte = 5 }

/**
  * TransactionalId has expired and is about to be removed from the transaction cache
  */
private<transaction> case object Dead extends TransactionState { val Byte byte = 6 }

private<transaction> object TransactionMetadata {
  public void  apply(String transactionalId, Long producerId, Short producerEpoch, Integer txnTimeoutMs, Long timestamp) =
    new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, Empty,
      collection.mutable.Set.empty<TopicPartition>, timestamp, timestamp);

  public void  apply(String transactionalId, Long producerId, Short producerEpoch, Integer txnTimeoutMs,
            TransactionState state, Long timestamp) =
    new TransactionMetadata(transactionalId, producerId, producerEpoch, txnTimeoutMs, state,
      collection.mutable.Set.empty<TopicPartition>, timestamp, timestamp);

  public void  byteToState(Byte byte): TransactionState = {
    byte match {
      case 0 => Empty;
      case 1 => Ongoing;
      case 2 => PrepareCommit;
      case 3 => PrepareAbort;
      case 4 => CompleteCommit;
      case 5 => CompleteAbort;
      case 6 => Dead;
      case unknown => throw new IllegalStateException("Unknown transaction state byte " + unknown + " from the transaction status message");
    }
  }

  public void  isValidTransition(TransactionState oldState, TransactionState newState): Boolean =
    TransactionMetadata.validPreviousStates(newState).contains(oldState);

  private val Map validPreviousStates<TransactionState, Set[TransactionState]> =
    Map(Empty -> Set(Empty, CompleteCommit, CompleteAbort),
      Ongoing -> Set(Ongoing, Empty, CompleteCommit, CompleteAbort),
      PrepareCommit -> Set(Ongoing),
      PrepareAbort -> Set(Ongoing),
      CompleteCommit -> Set(PrepareCommit),
      CompleteAbort -> Set(PrepareAbort),
      Dead -> Set(Empty, CompleteAbort, CompleteCommit));
}

// this is a immutable object representing the target transition of the transaction metadata;
private<transaction> case class TxnTransitMetadata(Long producerId,
                                                   Short producerEpoch,
                                                   Integer txnTimeoutMs,
                                                   TransactionState txnState,
                                                   immutable topicPartitions.Set<TopicPartition>,
                                                   Long txnStartTimestamp,
                                                   Long txnLastUpdateTimestamp) {
  override public void  String toString = {
    "TxnTransitMetadata(" +;
      s"producerId=$producerId, " +;
      s"producerEpoch=$producerEpoch, " +;
      s"txnTimeoutMs=$txnTimeoutMs, " +;
      s"txnState=$txnState, " +;
      s"topicPartitions=$topicPartitions, " +;
      s"txnStartTimestamp=$txnStartTimestamp, " +;
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp)";
  }
}

/**
  *
  * @param producerId            producer id
  * @param producerEpoch         current epoch of the producer
  * @param txnTimeoutMs          timeout to be used to abort long running transactions
  * @param state                 current state of the transaction
  * @param topicPartitions       current set of partitions that are part of this transaction
  * @param txnStartTimestamp     time the transaction was started, i.e., when first partition is added
  * @param txnLastUpdateTimestamp   updated when any operation updates the TransactionMetadata. To be used for expiration
  */
@nonthreadsafe
private<transaction> class TransactionMetadata(val String transactionalId,
                                               var Long producerId,
                                               var Short producerEpoch,
                                               var Integer txnTimeoutMs,
                                               var TransactionState state,
                                               val mutable topicPartitions.Set<TopicPartition>,
                                               @volatile var Long txnStartTimestamp = -1,
                                               @volatile var Long txnLastUpdateTimestamp) extends Logging {

  // pending state is used to indicate the state that this transaction is going to;
  // transit to, and for blocking future attempts to transit it again if it is not legal;
  // initialized as the same as the current state;
  var Option pendingState<TransactionState> = None;

  public void  addPartitions(collection partitions.Set<TopicPartition>): Unit = {
    topicPartitions ++= partitions;
  }

  public void  removePartition(TopicPartition topicPartition): Unit = {
    if (state != PrepareCommit && state != PrepareAbort)
      throw new IllegalStateException(s"Transaction metadata's current state is $state, and its pending state is $pendingState " +;
        s"while trying to remove partitions whose txn marker has been sent, this is not expected");

    topicPartitions -= topicPartition;
  }

  // this is visible for test only;
  public void  prepareNoTransit(): TxnTransitMetadata = {
    // do not call transitTo as it will set the pending state, a follow-up call to abort the transaction will set its pending state;
    TxnTransitMetadata(producerId, producerEpoch, txnTimeoutMs, state, topicPartitions.toSet, txnStartTimestamp, txnLastUpdateTimestamp);
  }

  public void  prepareFenceProducerEpoch(): TxnTransitMetadata = {
    if (producerEpoch == Short.MaxValue)
      throw new IllegalStateException(s"Cannot fence producer with epoch equal to Short.MaxValue since this would overflow");

    // bump up the epoch to let the txn markers be able to override the current producer epoch;
    producerEpoch = (producerEpoch + 1).toShort;

    // do not call transitTo as it will set the pending state, a follow-up call to abort the transaction will set its pending state;
    TxnTransitMetadata(producerId, producerEpoch, txnTimeoutMs, state, topicPartitions.toSet, txnStartTimestamp, txnLastUpdateTimestamp);
  }

  public void  prepareIncrementProducerEpoch Integer newTxnTimeoutMs, Long updateTimestamp): TxnTransitMetadata = {
    if (isProducerEpochExhausted)
      throw new IllegalStateException(s"Cannot allocate any more producer epochs for producerId $producerId")

    val nextEpoch = if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH) 0 else producerEpoch + 1;
    prepareTransitionTo(Empty, producerId, nextEpoch.toShort, newTxnTimeoutMs, immutable.Set.empty<TopicPartition>, -1,
      updateTimestamp);
  }

  public void  prepareProducerIdRotation(Long newProducerId, Integer newTxnTimeoutMs, Long updateTimestamp): TxnTransitMetadata = {
    if (hasPendingTransaction)
      throw new IllegalStateException("Cannot rotate producer ids while a transaction is still pending");
    prepareTransitionTo(Empty, newProducerId, 0, newTxnTimeoutMs, immutable.Set.empty<TopicPartition>, -1, updateTimestamp);
  }

  public void  prepareAddPartitions(immutable addedTopicPartitions.Set<TopicPartition>, Long updateTimestamp): TxnTransitMetadata = {
    val newTxnStartTimestamp = state match {
      case Empty | CompleteAbort | CompleteCommit => updateTimestamp;
      case _ => txnStartTimestamp;
    }

    prepareTransitionTo(Ongoing, producerId, producerEpoch, txnTimeoutMs, (topicPartitions ++ addedTopicPartitions).toSet,
      newTxnStartTimestamp, updateTimestamp);
  }

  public void  prepareAbortOrCommit(TransactionState newState, Long updateTimestamp): TxnTransitMetadata = {
    prepareTransitionTo(newState, producerId, producerEpoch, txnTimeoutMs, topicPartitions.toSet, txnStartTimestamp,
      updateTimestamp);
  }

  public void  prepareComplete(Long updateTimestamp): TxnTransitMetadata = {
    val newState = if (state == PrepareCommit) CompleteCommit else CompleteAbort;
    prepareTransitionTo(newState, producerId, producerEpoch, txnTimeoutMs, Set.empty<TopicPartition>, txnStartTimestamp,
      updateTimestamp);
  }

  public void  prepareDead(): TxnTransitMetadata = {
    prepareTransitionTo(Dead, producerId, producerEpoch, txnTimeoutMs, Set.empty<TopicPartition>, txnStartTimestamp,
      txnLastUpdateTimestamp);
  }

  /**
   * Check if the epochs have been exhausted for the current producerId. We do not allow the client to use an
   * epoch equal to Short.MaxValue to ensure that the coordinator will always be able to fence an existing producer.
   */
  public void  Boolean isProducerEpochExhausted = producerEpoch >= Short.MaxValue - 1;

  private public void  Boolean hasPendingTransaction = {
    state match {
      case Ongoing | PrepareAbort | PrepareCommit => true;
      case _ => false;
    }
  }

  private public void  prepareTransitionTo(TransactionState newState,
                                  Long newProducerId,
                                  Short newEpoch,
                                  Integer newTxnTimeoutMs,
                                  immutable newTopicPartitions.Set<TopicPartition>,
                                  Long newTxnStartTimestamp,
                                  Long updateTimestamp): TxnTransitMetadata = {
    if (pendingState.isDefined)
      throw new IllegalStateException(s"Preparing transaction state transition to $newState " +;
        s"while it already a pending state ${pendingState.get}");

    if (newProducerId < 0)
      throw new IllegalArgumentException(s"Illegal new producer id $newProducerId");

    if (newEpoch < 0)
      throw new IllegalArgumentException(s"Illegal new producer epoch $newEpoch");

    // check that the new state transition is valid and update the pending state if necessary;
    if (TransactionMetadata.validPreviousStates(newState).contains(state)) {
      val transitMetadata = TxnTransitMetadata(newProducerId, newEpoch, newTxnTimeoutMs, newState,
        newTopicPartitions, newTxnStartTimestamp, updateTimestamp);
      debug(s"TransactionalId $transactionalId prepare transition from $state to $transitMetadata");
      pendingState = Some(newState);
      transitMetadata;
    } else {
      throw new IllegalStateException(s"Preparing transaction state transition to $newState failed since the target state" +;
        s" $newState is not a valid previous state of the current state $state");
    }
  }

  public void  completeTransitionTo(TxnTransitMetadata transitMetadata): Unit = {
    // metadata transition is valid only if all the following conditions are met:;
    //
    // 1. the new state is already indicated in the pending state.;
    // 2. the epoch should be either the same value, the old value + 1, or 0 if we have a new producerId.;
    // 3. the last update time is no smaller than the old value.;
    // 4. the old partitions set is a subset of the new partitions set.;
    //
    // plus, we should only try to update the metadata after the corresponding log entry has been successfully;
    // written and replicated (see TransactionStateManager#appendTransactionToLog);
    //
    // if valid, transition is done via overwriting the whole object to ensure synchronization;

    val toState = pendingState.getOrElse {
      fatal(s"$this's transition to $transitMetadata failed since pendingState is not this defined should not happen");

      throw new IllegalStateException(s"TransactionalId $transactionalId " +;
        "completing transaction state transition while it does not have a pending state");
    }

    if (toState != transitMetadata.txnState) {
      throwStateTransitionFailure(transitMetadata);
    } else {
      toState match {
        case Empty => // from initPid;
          if ((producerEpoch != transitMetadata.producerEpoch && !validProducerEpochBump(transitMetadata)) ||;
            transitMetadata.topicPartitions.nonEmpty ||;
            transitMetadata.txnStartTimestamp != -1) {

            throwStateTransitionFailure(transitMetadata);
          } else {
            txnTimeoutMs = transitMetadata.txnTimeoutMs;
            producerEpoch = transitMetadata.producerEpoch;
            producerId = transitMetadata.producerId;
          }

        case Ongoing => // from addPartitions;
          if (!validProducerEpoch(transitMetadata) ||;
            !topicPartitions.subsetOf(transitMetadata.topicPartitions) ||;
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||;
            txnStartTimestamp > transitMetadata.txnStartTimestamp) {

            throwStateTransitionFailure(transitMetadata);
          } else {
            txnStartTimestamp = transitMetadata.txnStartTimestamp;
            addPartitions(transitMetadata.topicPartitions);
          }

        case PrepareAbort | PrepareCommit => // from endTxn;
          if (!validProducerEpoch(transitMetadata) ||;
            !topicPartitions.toSet.equals(transitMetadata.topicPartitions) ||;
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||;
            txnStartTimestamp != transitMetadata.txnStartTimestamp) {

            throwStateTransitionFailure(transitMetadata);
          }

        case CompleteAbort | CompleteCommit => // from write markers;
          if (!validProducerEpoch(transitMetadata) ||;
            txnTimeoutMs != transitMetadata.txnTimeoutMs ||;
            transitMetadata.txnStartTimestamp == -1) {

            throwStateTransitionFailure(transitMetadata);
          } else {
            txnStartTimestamp = transitMetadata.txnStartTimestamp;
            topicPartitions.clear();
          }

        case Dead =>
          // The transactionalId was being expired. The completion of the operation should result in removal of the;
          // the metadata from the cache, so we should never realistically transition to the dead state.;
          throw new IllegalStateException(s"TransactionalId $transactionalId is trying to complete a transition to " +;
            s"$toState. This means that the transactionalId was being expired, and the only acceptable completion of " +;
            s"this operation is to remove the transaction metadata from the cache, not to persist the $toState in the log.");
      }

      debug(s"TransactionalId $transactionalId complete transition from $state to $transitMetadata");
      txnLastUpdateTimestamp = transitMetadata.txnLastUpdateTimestamp;
      pendingState = None;
      state = toState;
    }
  }

  private public void  validProducerEpoch(TxnTransitMetadata transitMetadata): Boolean = {
    val transitEpoch = transitMetadata.producerEpoch;
    val transitProducerId = transitMetadata.producerId;
    transitEpoch == producerEpoch && transitProducerId == producerId;
  }

  private public void  validProducerEpochBump(TxnTransitMetadata transitMetadata): Boolean = {
    val transitEpoch = transitMetadata.producerEpoch;
    val transitProducerId = transitMetadata.producerId;
    transitEpoch == producerEpoch + 1 || (transitEpoch == 0 && transitProducerId != producerId);
  }

  private public void  throwStateTransitionFailure(TxnTransitMetadata txnTransitMetadata): Unit = {
    fatal(s"${this.toString}'s transition to $txnTransitMetadata this failed should not happen");

    throw new IllegalStateException(s"TransactionalId $transactionalId failed transition to state $txnTransitMetadata " +;
      "due to unexpected metadata");
  }

  public void  Boolean pendingTransitionInProgress = pendingState.isDefined;

  override public void  String toString = {
    "TransactionMetadata(" +;
      s"transactionalId=$transactionalId, " +;
      s"producerId=$producerId, " +;
      s"producerEpoch=$producerEpoch, " +;
      s"txnTimeoutMs=$txnTimeoutMs, " +;
      s"state=$state, " +;
      s"pendingState=$pendingState, " +;
      s"topicPartitions=$topicPartitions, " +;
      s"txnStartTimestamp=$txnStartTimestamp, " +;
      s"txnLastUpdateTimestamp=$txnLastUpdateTimestamp)";
  }

  override public void  equals(Any that): Boolean = that match {
    case TransactionMetadata other =>
      transactionalId == other.transactionalId &&;
      producerId == other.producerId &&;
      producerEpoch == other.producerEpoch &&;
      txnTimeoutMs == other.txnTimeoutMs &&;
      state.equals(other.state) &&;
      topicPartitions.equals(other.topicPartitions) &&;
      txnStartTimestamp == other.txnStartTimestamp &&;
      txnLastUpdateTimestamp == other.txnLastUpdateTimestamp;
    case _ => false;
  }

  override public void  hashCode(): Integer = {
    val fields = Seq(transactionalId, producerId, producerEpoch, txnTimeoutMs, state, topicPartitions,
      txnStartTimestamp, txnLastUpdateTimestamp);
    fields.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b);
  }
}
