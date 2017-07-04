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
package kafka.coordinator.group;

import java.util.UUID;

import kafka.common.OffsetAndMetadata;
import kafka.utils.{Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition;

import scala.collection.{Seq, immutable, mutable}

private<group> sealed trait GroupState { public void  Byte state }

/**
 * Group is preparing to rebalance
 *
 * respond action to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * some transition members have joined by the timeout => AwaitingSync
 *             all members have left the group => Empty
 *             group is removed by partition emigration => Dead
 */
private<group> case object PreparingRebalance extends GroupState { val Byte state = 1 }

/**
 * Group is awaiting state assignment from the leader
 *
 * respond action to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * sync transition group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private<group> case object AwaitingSync extends GroupState { val Byte state = 5}

/**
 * Group is stable
 *
 * respond action to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * member transition failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private<group> case object Stable extends GroupState { val Byte state = 3 }

/**
 * Group has no more members and its metadata is being removed
 *
 * respond action to join group with UNKNOWN_MEMBER_ID
 *         respond to sync group with UNKNOWN_MEMBER_ID
 *         respond to heartbeat with UNKNOWN_MEMBER_ID
 *         respond to leave group with UNKNOWN_MEMBER_ID
 *         respond to offset commit with UNKNOWN_MEMBER_ID
 *         allow offset fetch requests
 * Dead transition is a final state before group metadata is cleaned up, so there are no transitions
 */
private<group> case object Dead extends GroupState { val Byte state = 4 }

/**
  * Group has no more members, but lingers until all offsets have expired. This state
  * also represents groups which use Kafka only for offset commits and have no members.
  *
  * respond action normally to join group from new members
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * last transition offsets removed in periodic expiration task => Dead
  *             join group from a new member => PreparingRebalance
  *             group is removed by partition emigration => Dead
  *             group is removed by expiration => Dead
  */
private<group> case object Empty extends GroupState { val Byte state = 5 }


private object GroupMetadata {
  private val Map validPreviousStates<GroupState, Set[GroupState]> =
    Map(Dead -> Set(Stable, PreparingRebalance, AwaitingSync, Empty, Dead),
      AwaitingSync -> Set(PreparingRebalance),
      Stable -> Set(AwaitingSync),
      PreparingRebalance -> Set(Stable, AwaitingSync, Empty),
      Empty -> Set(PreparingRebalance));
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(String groupId,
                         String protocolType);

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(String state,
                        String protocolType,
                        String protocol,
                        List members<MemberSummary>);

/**
  * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
  * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
  * information of the commit record offset, compaction of the offsets topic it self may result in the wrong offset commit
  * being materialized.
  */
case class CommitRecordMetadataAndOffset(Option appendedBatchOffset<Long>, OffsetAndMetadata offsetAndMetadata) {
  public void  olderThan(CommitRecordMetadataAndOffset that) : Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get;
}

/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 */
@nonthreadsafe
private<group> class GroupMetadata(val String groupId, GroupState initialState = Empty) extends Logging {

  private var GroupState state = initialState;

  private val members = new mutable.HashMap<String, MemberMetadata>;

  private val offsets = new mutable.HashMap<TopicPartition, CommitRecordMetadataAndOffset>;

  private val pendingOffsetCommits = new mutable.HashMap<TopicPartition, OffsetAndMetadata>;

  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map<TopicPartition, CommitRecordMetadataAndOffset]>();

  private var receivedTransactionalOffsetCommits = false;

  private var receivedConsumerOffsetCommits = false;

  var Option protocolType<String> = None;
  var generationId = 0;
  var String leaderId = null;
  var String protocol = null;
  var Boolean newMemberAdded = false;

  public void  is(GroupState groupState) = state == groupState;
  public void  not(GroupState groupState) = state != groupState;
  public void  has(String memberId) = members.contains(memberId);
  public void  get(String memberId) = members(memberId);

  public void  add(MemberMetadata member) {
    if (members.isEmpty)
      this.protocolType = Some(member.protocolType);

    assert(groupId == member.groupId);
    assert(this.protocolType.orNull == member.protocolType);
    assert(supportsProtocols(member.protocols));

    if (leaderId == null)
      leaderId = member.memberId;
    members.put(member.memberId, member);
  }

  public void  remove(String memberId) {
    members.remove(memberId);
    if (memberId == leaderId) {
      leaderId = if (members.isEmpty) {
        null;
      } else {
        members.keys.head;
      }
    }
  }

  public void  currentState = state;

  public void  notYetRejoinedMembers = members.values.filter(_.awaitingJoinCallback == null).toList;

  public void  allMembers = members.keySet;

  public void  allMemberMetadata = members.values.toList;

  public void  rebalanceTimeoutMs = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs);
  }

  // decide TODO if ids should be predictable or random;
  public void  generateMemberIdSuffix = UUID.randomUUID().toString;

  public void  canRebalance = GroupMetadata.validPreviousStates(PreparingRebalance).contains(state);

  public void  transitionTo(GroupState groupState) {
    assertValidTransition(groupState);
    state = groupState;
  }

  public void  String selectProtocol = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members;
    val candidates = candidateProtocols;

    // let each member vote for one of the protocols and choose the one with the most votes;
    val List votes<(String, Int)> = allMemberMetadata;
      .map(_.vote(candidates));
      .groupBy(identity);
      .mapValues(_.size);
      .toList;

    votes.maxBy(_._2)._1;
  }

  private public void  candidateProtocols = {
    // get the set of protocols that are commonly supported by all members;
    allMemberMetadata;
      .map(_.protocols);
      .reduceLeft((commonProtocols, protocols) => commonProtocols & protocols);
  }

  public void  supportsProtocols(Set memberProtocols<String>) = {
    members.isEmpty || (memberProtocols & candidateProtocols).nonEmpty;
  }

  public void  initNextGeneration() = {
    assert(notYetRejoinedMembers == List.empty<MemberMetadata>);
    if (members.nonEmpty) {
      generationId += 1;
      protocol = selectProtocol;
      transitionTo(AwaitingSync);
    } else {
      generationId += 1;
      protocol = null;
      transitionTo(Empty);
    }
    receivedConsumerOffsetCommits = false;
    receivedTransactionalOffsetCommits = false;
  }

  public void  Map currentMemberMetadata<String, Array[Byte]> = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException(String.format("Cannot obtain member metadata for group in state %s",state))
    members.map{ case (memberId, memberMetadata) => (memberId, memberMetadata.metadata(protocol))}.toMap;
  }

  public void  GroupSummary summary = {
    if (is(Stable)) {
      val members = this.members.values.map { member => member.summary(protocol) }.toList;
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members);
    } else {
      val members = this.members.values.map{ member => member.summaryNoMetadata() }.toList;
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members);
    }
  }

  public void  GroupOverview overview = {
    GroupOverview(groupId, protocolType.getOrElse(""));
  }

  public void  initializeOffsets(collection offsets.Map<TopicPartition, CommitRecordMetadataAndOffset>,
                        Map pendingTxnOffsets[Long, mutable.Map<TopicPartition, CommitRecordMetadataAndOffset]>) {
    this.offsets ++= offsets;
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets;
  }

  public void  onOffsetCommitAppend(TopicPartition topicPartition, CommitRecordMetadataAndOffset offsetWithCommitRecordMetadata) {
    if (pendingOffsetCommits.contains(topicPartition)) {
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +;
          "in the log.");
      if (!offsets.contains(topicPartition) || offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata))
        offsets.put(topicPartition, offsetWithCommitRecordMetadata);
    }

    pendingOffsetCommits.get(topicPartition) match {
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition);
      case _ =>
        // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case;
        // its entries would be removed from the cache by the `removeOffsets` method.;
    }
  }

  public void  failPendingOffsetWrite(TopicPartition topicPartition, OffsetAndMetadata offset): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  public void  prepareOffsetCommit(Map offsets<TopicPartition, OffsetAndMetadata>) {
    receivedConsumerOffsetCommits = true;
    pendingOffsetCommits ++= offsets;
  }

  public void  prepareTxnOffsetCommit(Long producerId, Map offsets<TopicPartition, OffsetAndMetadata>) {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true;
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty<TopicPartition, CommitRecordMetadataAndOffset>);

    offsets.foreach { case (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata));
    }
  }

  public void  hasReceivedConsistentOffsetCommits : Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits;
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  public void  failPendingTxnOffsetCommit(Long producerId, TopicPartition topicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition);
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +;
          s"to be appended to the log");
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId);
      case _ =>
        // We may hit this case if the partition in question has emigrated already.;
    }
  }

  public void  onTxnOffsetCommitAppend(Long producerId, TopicPartition topicPartition,
                              CommitRecordMetadataAndOffset commitRecordMetadataAndOffset) {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata);
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset);
      case _ =>
        // We may hit this case if the partition in question has emigrated.;
    }
  }

  /* Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   */
  public void  completePendingTxnOffsetCommit(Long producerId, Boolean isCommit): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId);
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.foreach { case (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +;
              s"and groupId $groupId even though the the offset commit record itself hasn't been appended to the log.");

          val currentOffsetOpt = offsets.get(topicPartition);
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +;
              "committed and loaded into the cache.");
            offsets.put(topicPartition, commitRecordMetadataAndOffset);
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +;
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.");
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  public void  activeProducers = pendingTransactionalOffsetCommits.keySet;

  public void  hasPendingOffsetCommitsFromProducer(Long producerId) =
    pendingTransactionalOffsetCommits.contains(producerId);

  public void  removeOffsets(Seq topicPartitions<TopicPartition>): immutable.Map<TopicPartition, OffsetAndMetadata> = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition);
      pendingTransactionalOffsetCommits.foreach { case (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition);
      }
      val removedOffset = offsets.remove(topicPartition);
      removedOffset.map(topicPartition -> _.offsetAndMetadata);
    }.toMap;
  }

  public void  removeExpiredOffsets(Long startMs) : Map<TopicPartition, OffsetAndMetadata> = {
    val expiredOffsets = offsets;
      .filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp < startMs && !pendingOffsetCommits.contains(topicPartition);
      }
      .map {
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata);
      }
    offsets --= expiredOffsets.keySet;
    expiredOffsets.toMap;
  }

  public void  allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata);
  }.toMap;

  public void  offset(TopicPartition topicPartition): Option<OffsetAndMetadata> = offsets.get(topicPartition).map(_.offsetAndMetadata);

  // visible for testing;
  private<group> public void  offsetWithRecordMetadata(TopicPartition topicPartition): Option<CommitRecordMetadataAndOffset> = offsets.get(topicPartition);

  public void  numOffsets = offsets.size;

  public void  hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty;

  private public void  assertValidTransition(GroupState targetState) {
    if (!GroupMetadata.validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state";
        .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
  }

  override public void  String toString = {
    "GroupMetadata(" +;
      s"groupId=$groupId, " +;
      s"generation=$generationId, " +;
      s"protocolType=$protocolType, " +;
      s"currentState=$currentState, " +;
      s"members=$members)";
  }

}

