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

import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.utils._;

import scala.collection.Set;

object LeaderAndIsr {
  val Integer initialLeaderEpoch = 0;
  val Integer initialZKVersion = 0;
  val Integer NoLeader = -1;
  val Integer LeaderDuringDelete = -2;

  public void  apply Integer leader, List isr<Int>): LeaderAndIsr = LeaderAndIsr(leader, initialLeaderEpoch, isr, initialZKVersion);

  public void  duringDelete(List isr<Int>): LeaderAndIsr = LeaderAndIsr(LeaderDuringDelete, isr);
}

case class LeaderAndIsr Integer leader,
                        Integer leaderEpoch,
                        List isr<Int>,
                        Integer zkVersion) {
  public void  withZkVersion Integer zkVersion) = copy(zkVersion = zkVersion);

  public void  newLeader Integer leader) = newLeaderAndIsr(leader, isr);

  public void  newLeaderAndIsr Integer leader, List isr<Int>) = LeaderAndIsr(leader, leaderEpoch + 1, isr, zkVersion + 1);

  public void  newEpochAndZkVersion = newLeaderAndIsr(leader, isr);

  override public void  String toString = {
    Json.encode(Map("leader" -> leader, "leader_epoch" -> leaderEpoch, "isr" -> isr));
  }
}

case class PartitionStateInfo(LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch, Seq allReplicas<Int>) {

  override public void  String toString = {
    val partitionStateInfo = new StringBuilder;
    partitionStateInfo.append("(LeaderAndIsrInfo:" + leaderIsrAndControllerEpoch.toString);
    partitionStateInfo.append(",ReplicationFactor:" + allReplicas.size + ")");
    partitionStateInfo.append(",AllReplicas:" + allReplicas.mkString(",") + ")");
    partitionStateInfo.toString();
  }
}
