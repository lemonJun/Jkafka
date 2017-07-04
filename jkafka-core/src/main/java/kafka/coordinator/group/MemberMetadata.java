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

package kafka.coordinator.group;

import java.util;

import kafka.utils.nonthreadsafe;
import org.apache.kafka.common.protocol.Errors;


case class MemberSummary(String memberId,
                         String clientId,
                         String clientHost,
                         Array metadata<Byte>,
                         Array assignment<Byte>);

/**
 * Member metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance when callback the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync when callback the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 */
@nonthreadsafe
private<group> class MemberMetadata(val String memberId,
                                    val String groupId,
                                    val String clientId,
                                    val String clientHost,
                                    val Integer rebalanceTimeoutMs,
                                    val Integer sessionTimeoutMs,
                                    val String protocolType,
                                    var List supportedProtocols<(String, Array[Byte])>) {

  var Array assignment<Byte> = Array.empty<Byte>;
  var JoinGroupResult awaitingJoinCallback => Unit = null;
  var awaitingSyncCallback: (Array<Byte>, Errors) => Unit = null;
  var Long latestHeartbeat = -1;
  var Boolean isLeaving = false;

  public void  protocols = supportedProtocols.map(_._1).toSet;

  /**
   * Get metadata corresponding to the provided protocol.
   */
  public void  metadata(String protocol): Array<Byte> = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata;
      case None =>
        throw new IllegalArgumentException("Member does not support protocol");
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   */
  public void  matches(List protocols<(String, Array[Byte])>): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false;

    for (i <- protocols.indices) {
      val p1 = protocols(i);
      val p2 = supportedProtocols(i);
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false;
    }
    true;
  }

  public void  summary(String protocol): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, metadata(protocol), assignment);
  }

  public void  summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, Array.empty<Byte], Array.empty[Byte>);
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  public void  vote(Set candidates<String>): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol;
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols");
    }
  }

  override public void  String toString = {
    "MemberMetadata(" +;
      s"memberId=$memberId, " +;
      s"clientId=$clientId, " +;
      s"clientHost=$clientHost, " +;
      s"sessionTimeoutMs=$sessionTimeoutMs, " +;
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +;
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +;
      ")";
  }

}
