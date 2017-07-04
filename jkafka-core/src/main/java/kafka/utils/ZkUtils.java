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

package kafka.utils;

import java.util.concurrent.CountDownLatch;

import kafka.admin._;
import kafka.api.{ApiVersion, KAFKA_0_10_0_IV1, LeaderAndIsr}
import kafka.cluster._;
import kafka.common.{KafkaException, NoEpochForPartitionException, TopicAndPartition}
import kafka.consumer.{ConsumerThreadId, TopicCount}
import kafka.controller.{KafkaController, LeaderIsrAndControllerEpoch, ReassignedPartitionsContext}
import kafka.server.ConfigType;
import kafka.utils.ZkUtils._;
import org.I0Itec.zkclient.exception.{ZkBadVersionException, ZkException, ZkMarshallingError, ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.common.config.ConfigException;
import org.apache.zookeeper.AsyncCallback.{DataCallback, StringCallback}
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, KeeperException, ZooDefs, ZooKeeper}

import scala.collection._;
import scala.collection.JavaConverters._;

object ZkUtils {

  private val UseDefaultAcls = new java.util.ArrayList<ACL>;

  // it Important is necessary to add any new top level Zookeeper path here;
  val AdminPath = "/admin";
  val BrokersPath = "/brokers";
  val ClusterPath = "/cluster";
  val ConfigPath = "/config";
  val ControllerPath = "/controller";
  val ControllerEpochPath = "/controller_epoch";
  val IsrChangeNotificationPath = "/isr_change_notification";
  val KafkaAclPath = "/kafka-acl";
  val KafkaAclChangesPath = "/kafka-acl-changes";

  val ConsumersPath = "/consumers";
  val ClusterIdPath = s"$ClusterPath/id";
  val BrokerIdsPath = s"$BrokersPath/ids";
  val BrokerTopicsPath = s"$BrokersPath/topics";
  val ReassignPartitionsPath = s"$AdminPath/reassign_partitions";
  val DeleteTopicsPath = s"$AdminPath/delete_topics";
  val PreferredReplicaLeaderElectionPath = s"$AdminPath/preferred_replica_election";
  val BrokerSequenceIdPath = s"$BrokersPath/seqid";
  val ConfigChangesPath = s"$ConfigPath/changes";
  val ConfigUsersPath = s"$ConfigPath/users";
  val ProducerIdBlockPath = "/latest_producer_id_block";
  // it Important is necessary to add any new top level Zookeeper path to the Seq;
  val SecureZkRootPaths = Seq(AdminPath,
                              BrokersPath,
                              ClusterPath,
                              ConfigPath,
                              ControllerPath,
                              ControllerEpochPath,
                              IsrChangeNotificationPath,
                              KafkaAclPath,
                              KafkaAclChangesPath,
                              ProducerIdBlockPath);

  // it Important is necessary to add any new top level Zookeeper path that contains;
  //            sensitive information that should not be world readable to the Seq;
  val SensitiveZkRootPaths = Seq(ConfigUsersPath);

  public void  apply(String zkUrl, Integer sessionTimeout, Integer connectionTimeout, Boolean isZkSecurityEnabled): ZkUtils = {
    val (zkClient, zkConnection) = createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout);
    new ZkUtils(zkClient, zkConnection, isZkSecurityEnabled);
  }

  /*
   * Used in tests
   */
  public void  apply(ZkClient zkClient, Boolean isZkSecurityEnabled): ZkUtils = {
    new ZkUtils(zkClient, null, isZkSecurityEnabled);
  }

  public void  createZkClient(String zkUrl, Integer sessionTimeout, Integer connectionTimeout): ZkClient = {
    val zkClient = new ZkClient(zkUrl, sessionTimeout, connectionTimeout, ZKStringSerializer);
    zkClient;
  }

  public void  createZkClientAndConnection(String zkUrl, Integer sessionTimeout, Integer connectionTimeout): (ZkClient, ZkConnection) = {
    val zkConnection = new ZkConnection(zkUrl, sessionTimeout);
    val zkClient = new ZkClient(zkConnection, connectionTimeout, ZKStringSerializer);
    (zkClient, zkConnection);
  }

  public void  sensitivePath(String path): Boolean = {
    path != null && SensitiveZkRootPaths.exists(path.startsWith(_));
  }

  @deprecated("This is deprecated, use defaultAcls(isSecure, path) which doesn't make sensitive data world readable", since = "0.10.2.1")
  public void  DefaultAcls(Boolean isSecure): java.util.List<ACL> = public void aultAcls(isSecure, "");

  public void  public void aultAcls(Boolean isSecure, String path): java.util.List<ACL> = {
    if (isSecure) {
      val list = new java.util.ArrayList<ACL>;
      list.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
      if (!sensitivePath(path)) {
        list.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
      }
      list;
    } else;
      ZooDefs.Ids.OPEN_ACL_UNSAFE;
  }

  public void  maybeDeletePath(String zkUrl, String dir) {
    try {
      val zk = createZkClient(zkUrl, 30*1000, 30*1000);
      zk.deleteRecursive(dir);
      zk.close();
    } catch {
      case Throwable _ => // swallow;
    }
  }

  /*
   * Get calls that only depend on static paths
   */
  public void  getTopicPath(String topic): String = {
    ZkUtils.BrokerTopicsPath + "/" + topic;
  }

  public void  getTopicPartitionsPath(String topic): String = {
    getTopicPath(topic) + "/partitions";
  }

  public void  getTopicPartitionPath(String topic, Integer partitionId): String =
    getTopicPartitionsPath(topic) + "/" + partitionId;

  public void  getTopicPartitionLeaderAndIsrPath(String topic, Integer partitionId): String =
    getTopicPartitionPath(topic, partitionId) + "/" + "state";

  public void  getEntityConfigRootPath(String entityType): String =
    ZkUtils.ConfigPath + "/" + entityType;

  public void  getEntityConfigPath(String entityType, String entity): String =
    getEntityConfigRootPath(entityType) + "/" + entity;

  public void  getEntityConfigPath(String entityPath): String =
    ZkUtils.ConfigPath + "/" + entityPath;

  public void  getDeleteTopicPath(String topic): String =
    DeleteTopicsPath + "/" + topic;

  // Parses without deduplicating keys so the data can be checked before allowing reassignment to proceed;
  public void  parsePartitionReassignmentDataWithoutDedup(String jsonData): Seq<(TopicAndPartition, Seq[Int])> = {
    Json.parseFull(jsonData) match {
      case Some(m) =>
        m.asInstanceOf<Map[String, Any]>.get("partitions") match {
          case Some(partitionsSeq) =>
            partitionsSeq.asInstanceOf<Seq[Map[String, Any]]>.map(p => {
              val topic = p.get("topic").get.asInstanceOf<String>;
              val partition = p.get("partition").get.asInstanceOf<Int>;
              val newReplicas = p.get("replicas").get.asInstanceOf<Seq[Int]>;
              TopicAndPartition(topic, partition) -> newReplicas;
            });
          case None =>
            Seq.empty;
        }
      case None =>
        Seq.empty;
    }
  }

  public void  parsePartitionReassignmentData(String jsonData): Map<TopicAndPartition, Seq[Int]> =
    parsePartitionReassignmentDataWithoutDedup(jsonData).toMap;

  public void  parseTopicsData(String jsonData): Seq<String> = {
    var topics = List.empty<String>;
    Json.parseFull(jsonData) match {
      case Some(m) =>
        m.asInstanceOf<Map[String, Any]>.get("topics") match {
          case Some(partitionsSeq) =>
            val mapPartitionSeq = partitionsSeq.asInstanceOf<Seq[Map[String, Any]]>;
            mapPartitionSeq.foreach(p => {
              val topic = p.get("topic").get.asInstanceOf<String>;
              topics ++= List(topic);
            });
          case None =>
        }
      case None =>
    }
    topics;
  }

  public void  controllerZkData Integer brokerId, Long timestamp): String = {
    Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp.toString));
  }

  public void  preferredReplicaLeaderElectionZkData(scala partitions.collection.Set<TopicAndPartition>): String = {
    Json.encode(Map("version" -> 1, "partitions" -> partitions.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition))));
  }

  public void  formatAsReassignmentJson(Map partitionsToBeReassigned<TopicAndPartition, Seq[Int]>): String = {
    Json.encode(Map(
      "version" -> 1,
      "partitions" -> partitionsToBeReassigned.map { case (TopicAndPartition(topic, partition), replicas) =>
        Map(
          "topic" -> topic,
          "partition" -> partition,
          "replicas" -> replicas;
        );
      }
    ));
  }

}

class ZkUtils(val ZkClient zkClient,
              val ZkConnection zkConnection,
              val Boolean isSecure) extends Logging {
  // These are persistent ZK paths that should exist on kafka broker startup.;
  val persistentZkPaths = Seq(ConsumersPath,
                              BrokerIdsPath,
                              BrokerTopicsPath,
                              ConfigChangesPath,
                              getEntityConfigRootPath(ConfigType.Topic),
                              getEntityConfigRootPath(ConfigType.Client),
                              DeleteTopicsPath,
                              BrokerSequenceIdPath,
                              IsrChangeNotificationPath,
                              ProducerIdBlockPath);

  // Visible for testing;
  val zkPath = new ZkPath(zkClient);

  import ZkUtils._;

  @deprecated("This is deprecated, use defaultAcls(path) which doesn't make sensitive data world readable", since = "0.10.2.1")
  val java DefaultAcls.util.List<ACL> = ZkUtils.defaultAcls(isSecure, "");

  public void  public void aultAcls(String path): java.util.List<ACL> = ZkUtils.public void aultAcls(isSecure, path);

  public void  getController(): Integer = {
    readDataMaybeNull(ControllerPath)._1 match {
      case Some(controller) => KafkaController.parseControllerId(controller);
      case None => throw new KafkaException("Controller doesn't exist");
    }
  }

  /* Represents a cluster identifier. Stored in Zookeeper in JSON format: {"version" -> "1", "id" -> id } */
  object ClusterId {

    public void  toJson(String id) = {
      val jsonMap = Map("version" -> "1", "id" -> id);
      Json.encode(jsonMap);
    }

    public void  fromJson(String clusterIdJson): String = {
      Json.parseFull(clusterIdJson).map { m =>
        val clusterIdMap = m.asInstanceOf<Map[String, Any]>;
        clusterIdMap.get("id").get.asInstanceOf<String>;
      }.getOrElse(throw new KafkaException(s"Failed to parse the cluster id json $clusterIdJson"));
    }
  }

  public void  Option getClusterId<String> =
    readDataMaybeNull(ClusterIdPath)._1.map(ClusterId.fromJson);

  public void  createOrGetClusterId(String proposedClusterId): String = {
    try {
      createPersistentPath(ClusterIdPath, ClusterId.toJson(proposedClusterId));
      proposedClusterId;
    } catch {
      case ZkNodeExistsException _ =>
        getClusterId.getOrElse(throw new KafkaException("Failed to get cluster id from Zookeeper. This can only happen if /cluster/id is deleted from Zookeeper."))
    }
  }

  public void  getSortedBrokerList(): Seq<Int> =
    getChildren(BrokerIdsPath).map(_.toInt).sorted;

  public void  getAllBrokersInCluster(): Seq<Broker> = {
    val brokerIds = getChildrenParentMayNotExist(BrokerIdsPath).sorted;
    brokerIds.map(_.toInt).map(getBrokerInfo(_)).filter(_.isDefined).map(_.get);
  }

  public void  getLeaderAndIsrForPartition(String topic, Integer partition):Option<LeaderAndIsr> = {
    ReplicationUtils.getLeaderIsrAndEpochForPartition(this, topic, partition).map(_.leaderAndIsr);
  }

  public void  setupCommonPaths() {
    for(path <- persistentZkPaths)
      makeSurePersistentPathExists(path);
  }

  public void  getLeaderForPartition(String topic, Integer partition): Option<Int> = {
    readDataMaybeNull(getTopicPartitionLeaderAndIsrPath(topic, partition))._1.flatMap { leaderAndIsr =>
      Json.parseFull(leaderAndIsr).map(_.asInstanceOf<Map[String, Any]]("leader").asInstanceOf[Int>);
    }
  }

  /**
   * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
   * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
   * other broker will retry becoming leader with the same new epoch value.
   */
  public void  getEpochForPartition(String topic, Integer partition): Integer = {
    val leaderAndIsrOpt = readDataMaybeNull(getTopicPartitionLeaderAndIsrPath(topic, partition))._1;
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case None => throw new NoEpochForPartitionException(String.format("No epoch, leaderAndISR data for partition <%s,%d> is invalid",topic, partition))
          case Some(m) => m.asInstanceOf<Map[String, Any]].get("leader_epoch").get.asInstanceOf[Int>;
        }
      case None => throw new NoEpochForPartitionException("No epoch, ISR path for partition <%s,%d> is empty";
        .format(topic, partition))
    }
  }

  /** returns a sequence id generated by updating BrokerSequenceIdPath in Zk.
    * users can provide brokerId in the config , inorder to avoid conflicts between zk generated
    * seqId and config.brokerId we increment zk seqId by KafkaConfig.MaxReservedBrokerId.
    */
  public void  getBrokerSequenceId Integer MaxReservedBrokerId): Integer = {
    getSequenceId(BrokerSequenceIdPath) + MaxReservedBrokerId;
  }

  /**
   * Gets the in-sync replicas (ISR) for a specific topic and partition
   */
  public void  getInSyncReplicasForPartition(String topic, Integer partition): Seq<Int> = {
    val leaderAndIsrOpt = readDataMaybeNull(getTopicPartitionLeaderAndIsrPath(topic, partition))._1;
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case Some(m) => m.asInstanceOf<Map[String, Any]].get("isr").get.asInstanceOf[Seq[Int]>;
          case None => Seq.empty<Int>;
        }
      case None => Seq.empty<Int>;
    }
  }

  /**
   * Gets the assigned replicas (AR) for a specific topic and partition
   */
  public void  getReplicasForPartition(String topic, Integer partition): Seq<Int> = {
    val jsonPartitionMapOpt = readDataMaybeNull(getTopicPath(topic))._1;
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        Json.parseFull(jsonPartitionMap) match {
          case Some(m) => m.asInstanceOf<Map[String, Any]>.get("partitions") match {
            case Some(replicaMap) => replicaMap.asInstanceOf<Map[String, Seq[Int]]>.get(partition.toString) match {
              case Some(seq) => seq;
              case None => Seq.empty<Int>;
            }
            case None => Seq.empty<Int>;
          }
          case None => Seq.empty<Int>;
        }
      case None => Seq.empty<Int>;
    }
  }

  /**
   * Register brokers with v4 json format (which includes multiple endpoints and rack) if
   * the apiVersion is 0.10.0.X or above. Register the broker with v2 json format otherwise.
   * Due to KAFKA-3100, 0.9.0.0 broker and old clients will break if JSON version is above 2.
   * We include v2 to make it possible for the broker to migrate from 0.9.0.0 to 0.10.0.X or above without having to
   * upgrade to 0.9.0.1 first (clients have to be upgraded to 0.9.0.1 in any case).
   *
   * This format also includes default endpoints for compatibility with older clients.
   *
   * @param id broker ID
   * @param host broker host name
   * @param port broker port
   * @param advertisedEndpoints broker end points
   * @param jmxPort jmx port
   * @param rack broker rack
   * @param apiVersion Kafka version the broker is running as
   */
  public void  registerBrokerInZk Integer id,
                         String host,
                         Integer port,
                         Seq advertisedEndpoints<EndPoint>,
                         Integer jmxPort,
                         Option rack<String>,
                         ApiVersion apiVersion) {
    val brokerIdPath = BrokerIdsPath + "/" + id;
    // see method documentation for reason why we do this;
    val version = if (apiVersion >= KAFKA_0_10_0_IV1) 4 else 2;
    val json = Broker.toJson(version, id, host, port, advertisedEndpoints, jmxPort, rack);
    registerBrokerInZk(brokerIdPath, json);

    info(String.format("Registered broker %d at path %s with addresses: %s",id, brokerIdPath, advertisedEndpoints.mkString(",")))
  }

  private public void  registerBrokerInZk(String brokerIdPath, String brokerInfo) {
    try {
      val zkCheckedEphemeral = new ZKCheckedEphemeral(brokerIdPath,
                                                      brokerInfo,
                                                      zkConnection.getZookeeper,
                                                      isSecure);
      zkCheckedEphemeral.create();
    } catch {
      case ZkNodeExistsException _ =>
        throw new RuntimeException("A broker is already registered on the path " + brokerIdPath;
                + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or ";
                + "else you have shutdown this broker and restarted it faster than the zookeeper ";
                + "timeout so it appears to be re-registering.");
    }
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  public void  getConsumerPartitionOwnerPath(String group, String topic, Integer partition): String = {
    val topicDirs = new ZKGroupTopicDirs(group, topic);
    topicDirs.consumerOwnerDir + "/" + partition;
  }

  public void  leaderAndIsrZkData(LeaderAndIsr leaderAndIsr, Integer controllerEpoch): String = {
    Json.encode(Map("version" -> 1, "leader" -> leaderAndIsr.leader, "leader_epoch" -> leaderAndIsr.leaderEpoch,
                    "controller_epoch" -> controllerEpoch, "isr" -> leaderAndIsr.isr));
  }

  /**
   * Get JSON partition to replica map from zookeeper.
   */
  public void  replicaAssignmentZkData(Map map<String, Seq[Int]>): String = {
    Json.encode(Map("version" -> 1, "partitions" -> map));
  }

  /**
   *  make sure a persistent path exists in ZK. Create the path if not exist.
   */
  public void  makeSurePersistentPathExists(String path, java acls.util.List<ACL> = UseDefaultAcls) {
    //Consumer path is kept open as different consumers will write under this node.;
    val acl = if (path == null || path.isEmpty || path.equals(ConsumersPath)) {
      ZooDefs.Ids.OPEN_ACL_UNSAFE;
    } else if (acls eq UseDefaultAcls) {
      ZkUtils.defaultAcls(isSecure, path);
    } else {
      acls;
    }

    if (!zkClient.exists(path))
      zkPath.createPersistent(path, createParents = true, acl) //won't throw NoNodeException or NodeExistsException;
  }

  /**
   *  create the parent path
   */
  private public void  createParentPath(String path, java acls.util.List<ACL> = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls;
    val parentDir = path.substring(0, path.lastIndexOf('/'));
    if (parentDir.length != 0) {
      zkPath.createPersistent(parentDir, createParents = true, acl);
    }
  }

  /**
   * Create an ephemeral node with the given path and data. Create parents if necessary.
   */
  private public void  createEphemeralPath(String path, String data, java acls.util.List<ACL> = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls;
    try {
      zkPath.createEphemeral(path, data, acl);
    } catch {
      case ZkNoNodeException _ =>
        createParentPath(path);
        zkPath.createEphemeral(path, data, acl);
    }
  }

  /**
   * Create an ephemeral node with the given path and data.
   * Throw NodeExistException if node already exists.
   */
  public void  createEphemeralPathExpectConflict(String path, String data, java acls.util.List<ACL> = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls;
    try {
      createEphemeralPath(path, data, acl);
    } catch {
      case ZkNodeExistsException e =>
        // this can happen when there is connection loss; make sure the data is what we intend to write;
        var String storedData = null;
        try {
          storedData = readData(path)._1;
        } catch {
          case ZkNoNodeException _ => // the node disappeared; treat as if node existed and let caller handles this;
        }
        if (storedData == null || storedData != data) {
          info("conflict in " + path + " data: " + data + " stored data: " + storedData);
          throw e;
        } else {
          // otherwise, the creation succeeded, return normally;
          info(path + " exists with value " + data + " during connection loss; this is ok");
        }
    }
  }

  /**
   * Create a persistent node with the given path and data. Create parents if necessary.
   */
  public void  createPersistentPath(String path, String data = "", java acls.util.List<ACL> = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls;
    try {
      zkPath.createPersistent(path, data, acl);
    } catch {
      case ZkNoNodeException _ =>
        createParentPath(path);
        zkPath.createPersistent(path, data, acl);
    }
  }

  public void  createSequentialPersistentPath(String path, String data = "", java acls.util.List<ACL> = UseDefaultAcls): String = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls;
    zkPath.createPersistentSequential(path, data, acl);
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parent directory if necessary. Never throw NodeExistException.
   * Return the updated path zkVersion
   */
  public void  updatePersistentPath(String path, String data, java acls.util.List<ACL> = UseDefaultAcls) = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls;
    try {
      zkClient.writeData(path, data);
    } catch {
      case ZkNoNodeException _ =>
        createParentPath(path);
        try {
          zkPath.createPersistent(path, data, acl);
        } catch {
          case ZkNodeExistsException _ =>
            zkClient.writeData(path, data);
        }
    }
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
   * exist, the current version is not the expected version, etc.) return (false, -1)
   *
   * When there is a ConnectionLossException during the conditional update, zkClient will retry the update and may fail
   * since the previous update may have succeeded (but the stored zkVersion no longer matches the expected one).
   * In this case, we will run the optionalChecker to further check if the previous write did indeed succeeded.
   */
  public void  conditionalUpdatePersistentPath(String path, String data, Integer expectVersion,
    Option optionalChecker[(ZkUtils, String, String) => (Boolean,Int)] = None): (Boolean, Int) = {
    try {
      val stat = zkClient.writeDataReturnStat(path, data, expectVersion);
      debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d";
        .format(path, data, expectVersion, stat.getVersion))
      (true, stat.getVersion);
    } catch {
      case ZkBadVersionException e1 =>
        optionalChecker match {
          case Some(checker) => checker(this, path, data);
          case _ =>
            debug("Checker method is not passed skipping zkData match");
            debug("Conditional update of path %s with data %s and expected version %d failed due to %s";
              .format(path, data,expectVersion, e1.getMessage))
            (false, -1);
        }
      case Exception e2 =>
        debug(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s",path, data,
          expectVersion, e2.getMessage));
        (false, -1);
    }
  }

  /**
   * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the current
   * version is not the expected version, etc.) return (false, -1). If path doesn't exist, throws ZkNoNodeException
   */
  public void  conditionalUpdatePersistentPathIfExists(String path, String data, Integer expectVersion): (Boolean, Int) = {
    try {
      val stat = zkClient.writeDataReturnStat(path, data, expectVersion);
      debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d";
        .format(path, data, expectVersion, stat.getVersion))
      (true, stat.getVersion);
    } catch {
      case ZkNoNodeException nne => throw nne;
      case Exception e =>
        error(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s",path, data,
          expectVersion, e.getMessage));
        (false, -1);
    }
  }

  /**
   * Update the value of a persistent node with the given path and data.
   * create parent directory if necessary. Never throw NodeExistException.
   */
  public void  updateEphemeralPath(String path, String data, java acls.util.List<ACL> = UseDefaultAcls): Unit = {
    val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls;
    try {
      zkClient.writeData(path, data);
    } catch {
      case ZkNoNodeException _ =>
        createParentPath(path);
        zkPath.createEphemeral(path, data, acl);
    }
  }

  public void  deletePath(String path): Boolean = {
    try {
      zkClient.delete(path);
    } catch {
      case ZkNoNodeException _ =>
        // this can happen during a connection loss event, return normally;
        info(path + " deleted during connection loss; this is ok");
        false;
    }
  }

  /**
    * Conditional delete the persistent path data, return true if it succeeds,
    * false otherwise (the current version is not the expected version)
    */
   public void  conditionalDeletePath(String path, Integer expectedVersion): Boolean = {
    try {
      zkClient.delete(path, expectedVersion);
      true;
    } catch {
      case ZkBadVersionException _ => false;
    }
  }

  public void  deletePathRecursive(String path) {
    try {
      zkClient.deleteRecursive(path);
    } catch {
      case ZkNoNodeException _ =>
        // this can happen during a connection loss event, return normally;
        info(path + " deleted during connection loss; this is ok");
    }
  }

  public void  readData(String path): (String, Stat) = {
    val Stat stat = new Stat();
    val String dataStr = zkClient.readData(path, stat);
    (dataStr, stat);
  }

  public void  readDataMaybeNull(String path): (Option<String>, Stat) = {
    val stat = new Stat();
    val dataAndStat = try {
                        (Some(zkClient.readData(path, stat)), stat);
                      } catch {
                        case ZkNoNodeException _ =>
                          (None, stat);
                      }
    dataAndStat;
  }

  public void  readDataAndVersionMaybeNull(String path): (Option<String>, Int) = {
    val stat = new Stat();
    try {
      val String data = zkClient.readData(path, stat);
      (Option(data), stat.getVersion);
    } catch {
      case ZkNoNodeException _ => (None, stat.getVersion);
    }
  }

  public void  getChildren(String path): Seq<String> = zkClient.getChildren(path).asScala;

  public void  getChildrenParentMayNotExist(String path): Seq<String> = {
    try {
      zkClient.getChildren(path).asScala;
    } catch {
      case ZkNoNodeException _ => Nil;
    }
  }

  /**
   * Check if the given path exists
   */
  public void  pathExists(String path): Boolean = {
    zkClient.exists(path);
  }

  public void  isTopicMarkedForDeletion(String topic): Boolean = {
    pathExists(getDeleteTopicPath(topic));
  }

  public void  getCluster(): Cluster = {
    val cluster = new Cluster;
    val nodes = getChildrenParentMayNotExist(BrokerIdsPath);
    for (node <- nodes) {
      val brokerZKString = readData(BrokerIdsPath + "/" + node)._1;
      cluster.add(Broker.createBroker(node.toInt, brokerZKString));
    }
    cluster;
  }

  public void  getPartitionLeaderAndIsrForTopics(Set topicAndPartitions<TopicAndPartition>): mutable.Map<TopicAndPartition, LeaderIsrAndControllerEpoch> = {
    val ret = new mutable.HashMap<TopicAndPartition, LeaderIsrAndControllerEpoch>;
    for(topicAndPartition <- topicAndPartitions) {
      ReplicationUtils.getLeaderIsrAndEpochForPartition(this, topicAndPartition.topic, topicAndPartition.partition) match {
        case Some(leaderIsrAndControllerEpoch) => ret.put(topicAndPartition, leaderIsrAndControllerEpoch);
        case None =>
      }
    }
    ret;
  }

  public void  getReplicaAssignmentForTopics(Seq topics<String>): mutable.Map<TopicAndPartition, Seq[Int]> = {
    val ret = new mutable.HashMap<TopicAndPartition, Seq[Int]>;
    topics.foreach { topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(getTopicPath(topic))._1;
      jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
          Json.parseFull(jsonPartitionMap) match {
            case Some(m) => m.asInstanceOf<Map[String, Any]>.get("partitions") match {
              case Some(repl)  =>
                val replicaMap = repl.asInstanceOf<Map[String, Seq[Int]]>;
                for((partition, replicas) <- replicaMap){
                  ret.put(TopicAndPartition(topic, partition.toInt), replicas);
                  debug(String.format("Replicas assigned to topic <%s>, partition <%s> are <%s>",topic, partition, replicas))
                }
              case None =>
            }
            case None =>
          }
        case None =>
      }
    }
    ret;
  }

  public void  getPartitionAssignmentForTopics(Seq topics<String>): mutable.Map[String, collection.Map<Int, Seq[Int]]> = {
    val ret = new mutable.HashMap[String, Map<Int, Seq[Int]]>();
    topics.foreach{ topic =>
      val jsonPartitionMapOpt = readDataMaybeNull(getTopicPath(topic))._1;
      val partitionMap = jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
          Json.parseFull(jsonPartitionMap) match {
            case Some(m) => m.asInstanceOf<Map[String, Any]>.get("partitions") match {
              case Some(replicaMap) =>
                val m1 = replicaMap.asInstanceOf<Map[String, Seq[Int]]>;
                m1.map(p => (p._1.toInt, p._2));
              case None => Map<Int, Seq[Int]>();
            }
            case None => Map<Int, Seq[Int]>();
          }
        case None => Map<Int, Seq[Int]>();
      }
      debug(String.format("Partition map for /brokers/topics/%s is %s",topic, partitionMap))
      ret += (topic -> partitionMap);
    }
    ret;
  }

  public void  getPartitionsForTopics(Seq topics<String>): mutable.Map<String, Seq[Int]> = {
    getPartitionAssignmentForTopics(topics).map { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1;
      val partitionMap = topicAndPartitionMap._2;
      debug(String.format("partition assignment of /brokers/topics/%s is %s",topic, partitionMap))
      topic -> partitionMap.keys.toSeq.sortWith((s, t) => s < t);
    }
  }

  public void  getTopicPartitionCount(String topic): Option<Int> = {
    val topicData = getPartitionAssignmentForTopics(Seq(topic));
    if (topicData(topic).nonEmpty)
      Some(topicData(topic).size);
    else;
      None;
  }

  public void  getPartitionsBeingReassigned(): Map<TopicAndPartition, ReassignedPartitionsContext> = {
    // read the partitions and their new replica list;
    val jsonPartitionMapOpt = readDataMaybeNull(ReassignPartitionsPath)._1;
    jsonPartitionMapOpt match {
      case Some(jsonPartitionMap) =>
        val reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap);
        reassignedPartitions.map(p => p._1 -> new ReassignedPartitionsContext(p._2));
      case None => Map.empty<TopicAndPartition, ReassignedPartitionsContext>;
    }
  }

  public void  updatePartitionReassignmentData(Map partitionsToBeReassigned<TopicAndPartition, Seq[Int]>) {
    val zkPath = ReassignPartitionsPath;
    partitionsToBeReassigned.size match {
      case 0 => // need to delete the /admin/reassign_partitions path;
        deletePath(zkPath);
        info(String.format("No more partitions need to be reassigned. Deleting zk path %s",zkPath))
      case _ =>
        val jsonData = formatAsReassignmentJson(partitionsToBeReassigned)
        try {
          updatePersistentPath(zkPath, jsonData);
          debug(String.format("Updated partition reassignment path with %s",jsonData))
        } catch {
          case ZkNoNodeException _ =>
            createPersistentPath(zkPath, jsonData);
            debug(String.format("Created path %s with %s for partition reassignment",zkPath, jsonData))
          case Throwable e2 => throw new AdminOperationException(e2.toString);
        }
    }
  }

  public void  getPartitionsUndergoingPreferredReplicaElection(): Set<TopicAndPartition> = {
    // read the partitions and their new replica list;
    val jsonPartitionListOpt = readDataMaybeNull(PreferredReplicaLeaderElectionPath)._1;
    jsonPartitionListOpt match {
      case Some(jsonPartitionList) => PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(jsonPartitionList);
      case None => Set.empty<TopicAndPartition>;
    }
  }

  public void  deletePartition Integer brokerId, String topic) {
    val brokerIdPath = BrokerIdsPath + "/" + brokerId;
    zkClient.delete(brokerIdPath);
    val brokerPartTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic + "/" + brokerId;
    zkClient.delete(brokerPartTopicPath);
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  public void  getConsumersInGroup(String group): Seq<String> = {
    val dirs = new ZKGroupDirs(group);
    getChildren(dirs.consumerRegistryDir);
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  public void  getConsumersPerTopic(String group, Boolean excludeInternalTopics): mutable.Map<String, List[ConsumerThreadId]> = {
    val dirs = new ZKGroupDirs(group);
    val consumers = getChildrenParentMayNotExist(dirs.consumerRegistryDir);
    val consumersPerTopicMap = new mutable.HashMap<String, List[ConsumerThreadId]>;
    for (consumer <- consumers) {
      val topicCount = TopicCount.constructTopicCount(group, consumer, this, excludeInternalTopics);
      for ((topic, consumerThreadIdSet) <- topicCount.getConsumerThreadIdsPerTopic) {
        for (consumerThreadId <- consumerThreadIdSet)
          consumersPerTopicMap.get(topic) match {
            case Some(curConsumers) => consumersPerTopicMap.put(topic, consumerThreadId :: curConsumers);
            case _ => consumersPerTopicMap.put(topic, List(consumerThreadId));
          }
      }
    }
    for ( (topic, consumerList) <- consumersPerTopicMap )
      consumersPerTopicMap.put(topic, consumerList.sortWith((s,t) => s < t));
    consumersPerTopicMap;
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  public void  getTopicsPerMemberId(String group, Boolean excludeInternalTopics = true): Map<String, List[String]> = {
    val dirs = new ZKGroupDirs(group);
    val memberIds = getChildrenParentMayNotExist(dirs.consumerRegistryDir);
    memberIds.map { memberId =>
      val topicCount = TopicCount.constructTopicCount(group, memberId, this, excludeInternalTopics);
      memberId -> topicCount.getTopicCountMap.keys.toList;
    }.toMap;
  }

  /**
   * This API takes in a broker id, queries zookeeper for the broker metadata and returns the metadata for that broker
   * or throws an exception if the broker dies before the query to zookeeper finishes
   *
   * @param brokerId The broker id
   * @return An optional Broker object encapsulating the broker metadata
   */
  public void  getBrokerInfo Integer brokerId): Option<Broker> = {
    readDataMaybeNull(BrokerIdsPath + "/" + brokerId)._1 match {
      case Some(brokerInfo) => Some(Broker.createBroker(brokerId, brokerInfo));
      case None => None;
    }
  }

  /**
    * This API produces a sequence number by creating / updating given path in zookeeper
    * It uses the stat returned by the zookeeper and return the version. Every time
    * client updates the path stat.version gets incremented. Starting value of sequence number is 1.
    */
  public void  getSequenceId(String path, java acls.util.List<ACL> = UseDefaultAcls): Integer = {
    val acl = if (acls == UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls;
    public void  Integer writeToZk = zkClient.writeDataReturnStat(path, "", -1).getVersion;
    try {
      writeToZk;
    } catch {
      case ZkNoNodeException _ =>
        makeSurePersistentPathExists(path, acl);
        writeToZk;
    }
  }

  public void  getAllTopics(): Seq<String> = {
    val topics = getChildrenParentMayNotExist(BrokerTopicsPath);
    if(topics == null)
      Seq.empty<String>;
    else;
      topics;
  }

  /**
   * Returns all the entities whose configs have been overridden.
   */
  public void  getAllEntitiesWithConfig(String entityType): Seq<String> = {
    val entities = getChildrenParentMayNotExist(getEntityConfigRootPath(entityType));
    if(entities == null)
      Seq.empty<String>;
    else;
      entities;
  }

  public void  getAllPartitions(): Set<TopicAndPartition> = {
    val topics = getChildrenParentMayNotExist(BrokerTopicsPath);
    if (topics == null) Set.empty<TopicAndPartition>;
    else {
      topics.flatMap { topic =>
        // The partitions path may not exist if the topic is in the process of being deleted;
        getChildrenParentMayNotExist(getTopicPartitionsPath(topic)).map(_.toInt).map(TopicAndPartition(topic, _));
      }.toSet;
    }
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  public void  getConsumerGroups() = {
    getChildren(ConsumersPath);
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  public void  getTopicsByConsumerGroup(String consumerGroup) = {
    getChildrenParentMayNotExist(new ZKGroupDirs(consumerGroup).consumerGroupOwnersDir);
  }

  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  public void  getAllConsumerGroupsForTopic(String topic): Set<String> = {
    val groups = getChildrenParentMayNotExist(ConsumersPath);
    if (groups == null) Set.empty;
    else {
      groups.foldLeft(Set.empty<String>) {(consumerGroupsForTopic, group) =>
        val topics = getChildren(new ZKGroupDirs(group).consumerGroupOffsetsDir);
        if (topics.contains(topic)) consumerGroupsForTopic + group;
        else consumerGroupsForTopic;
      }
    }
  }

  public void  close() {
    if(zkClient != null) {
      zkClient.close();
    }
  }
}

private object ZKStringSerializer extends ZkSerializer {

  @throws(classOf<ZkMarshallingError>)
  public void  serialize(data : Object): Array<Byte> = data.asInstanceOf<String>.getBytes("UTF-8");

  @throws(classOf<ZkMarshallingError>)
  public void  deserialize(bytes : Array<Byte>): Object = {
    if (bytes == null)
      null;
    else;
      new String(bytes, "UTF-8");
  }
}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class ZKGroupDirs(val String group) {
  public void  consumerDir = ConsumersPath;
  public void  consumerGroupDir = consumerDir + "/" + group;
  public void  consumerRegistryDir = consumerGroupDir + "/ids";
  public void  consumerGroupOffsetsDir = consumerGroupDir + "/offsets";
  public void  consumerGroupOwnersDir = consumerGroupDir + "/owners";
}

@deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
class ZKGroupTopicDirs(String group, String topic) extends ZKGroupDirs(group) {
  public void  consumerOffsetDir = consumerGroupOffsetsDir + "/" + topic;
  public void  consumerOwnerDir = consumerGroupOwnersDir + "/" + topic;
}

object ZKConfig {
  val ZkConnectProp = "zookeeper.connect";
  val ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms";
  val ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms";
  val ZkSyncTimeMsProp = "zookeeper.sync.time.ms";
}

class ZKConfig(VerifiableProperties props) {
  import ZKConfig._;

  /** ZK host string */
  val zkConnect = props.getString(ZkConnectProp);

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = props.getInt(ZkSessionTimeoutMsProp, 6000);

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = props.getInt(ZkConnectionTimeoutMsProp, zkSessionTimeoutMs);

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = props.getInt(ZkSyncTimeMsProp, 2000);
}

class ZkPath(ZkClient client) {

  @volatile private var Boolean isNamespacePresent = false

  public void  checkNamespace() {
    if (isNamespacePresent)
      return;

    if (!client.exists("/")) {
      throw new ConfigException("Zookeeper namespace does not exist");
    }
    isNamespacePresent = true;
  }

  public void  resetNamespaceCheckedState() {
    isNamespacePresent = false;
  }

  public void  createPersistent(String path, Object data, java acls.util.List<ACL>) {
    checkNamespace();
    client.createPersistent(path, data, acls);
  }

  public void  createPersistent(String path, Boolean createParents, java acls.util.List<ACL>) {
    checkNamespace();
    client.createPersistent(path, createParents, acls);
  }

  public void  createEphemeral(String path, Object data, java acls.util.List<ACL>) {
    checkNamespace();
    client.createEphemeral(path, data, acls);
  }

  public void  createPersistentSequential(String path, Object data, java acls.util.List<ACL>): String = {
    checkNamespace();
    client.createPersistentSequential(path, data, acls);
  }
}

/**
 * Creates an ephemeral znode checking the session owner
 * in the case of conflict. In the regular case, the
 * znode is created and the create call returns OK. If
 * the call receives a node exists event, then it checks
 * if the session matches. If it does, then it returns OK,
 * and otherwise it fails the operation.
 */

class ZKCheckedEphemeral(String path,
                         String data,
                         ZooKeeper zkHandle,
                         Boolean isSecure) extends Logging {
  private val createCallback = new CreateCallback;
  private val getDataCallback = new GetDataCallback;
  val CountDownLatch latch = new CountDownLatch(1);
  var Code result = Code.OK;
  val defaultAcls = ZkUtils.defaultAcls(isSecure, path);

  private class CreateCallback extends StringCallback {
    public void  processResult Integer rc,
                      String path,
                      Object ctx,
                      String name) {
      Code.get(rc) match {
        case Code.OK =>
           setResult(Code.OK);
        case Code.CONNECTIONLOSS =>
          // try again;
          createEphemeral;
        case Code.NONODE =>
          error(String.format("No node for path %s (could be the parent missing)",path))
          setResult(Code.NONODE);
        case Code.NODEEXISTS =>
          zkHandle.getData(path, false, getDataCallback, null);
        case Code.SESSIONEXPIRED =>
          error(String.format("Session has expired while creating %s",path))
          setResult(Code.SESSIONEXPIRED);
        case Code.INVALIDACL =>
          error("Invalid ACL");
          setResult(Code.INVALIDACL);
        case _ =>
          warn(String.format("ZooKeeper event while creating registration node: %s %s",path, Code.get(rc)))
          setResult(Code.get(rc));
      }
    }
  }

  private class GetDataCallback extends DataCallback {
      public void  processResult Integer rc,
                        String path,
                        Object ctx,
                        Array readData<Byte>,
                        Stat stat) {
        Code.get(rc) match {
          case Code.OK =>
                if (stat.getEphemeralOwner != zkHandle.getSessionId)
                  setResult(Code.NODEEXISTS);
                else;
                  setResult(Code.OK);
          case Code.NONODE =>
            info(String.format("The ephemeral node <%s> at %s has gone away while reading it, ",data, path))
            createEphemeral;
          case Code.SESSIONEXPIRED =>
            error(String.format("Session has expired while reading znode %s",path))
            setResult(Code.SESSIONEXPIRED);
          case Code.INVALIDACL =>
            error("Invalid ACL");
            setResult(Code.INVALIDACL);
          case _ =>
            warn(String.format("ZooKeeper event while getting znode data: %s %s",path, Code.get(rc)))
            setResult(Code.get(rc));
        }
      }
  }

  private public void  createEphemeral() {
    zkHandle.create(path,
                    ZKStringSerializer.serialize(data),
                    defaultAcls,
                    CreateMode.EPHEMERAL,
                    createCallback,
                    null);
  }

  private public void  createRecursive(String prefix, String suffix) {
    debug(String.format("Path: %s, Prefix: %s, Suffix: %s",path, prefix, suffix))
    if(suffix.isEmpty()) {
      createEphemeral;
    } else {
      zkHandle.create(prefix,
                      new Array<Byte>(0),
                      defaultAcls,
                      CreateMode.PERSISTENT,
                      new StringCallback() {
                        public void  processResult(rc : Int,
                                          path : String,
                                          ctx : Object,
                                          name : String) {
                          Code.get(rc) match {
                            case Code.OK | Code.NODEEXISTS =>
                              // Nothing to do;
                            case Code.CONNECTIONLOSS =>
                              // try again;
                              val suffix = ctx.asInstanceOf<String>;
                              createRecursive(path, suffix);
                            case Code.NONODE =>
                              error(String.format("No node for path %s (could be the parent missing)",path))
                              setResult(Code.get(rc));
                            case Code.SESSIONEXPIRED =>
                              error(String.format("Session has expired while creating %s",path))
                              setResult(Code.get(rc));
                            case Code.INVALIDACL =>
                              error("Invalid ACL");
                              setResult(Code.INVALIDACL);
                            case _ =>
                              warn(String.format("ZooKeeper event while creating registration node: %s %s",path, Code.get(rc)))
                              setResult(Code.get(rc));
                          }
                        }
                  },
                  suffix);
      // Update prefix and suffix;
      val index = suffix.indexOf('/', 1) match {
        case -1 => suffix.length;
        case x : Integer => x;
      }
      // Get new prefix;
      val newPrefix = prefix + suffix.substring(0, index);
      // Get new suffix;
      val newSuffix = suffix.substring(index, suffix.length);
      createRecursive(newPrefix, newSuffix);
    }
  }

  private public void  setResult(Code code) {
    result = code;
    latch.countDown();
  }

  private public void  waitUntilResolved(): Code = {
    latch.await();
    result;
  }

  public void  create() {
    val index = path.indexOf('/', 1) match {
        case -1 => path.length;
        case x : Integer => x;
    }
    val prefix = path.substring(0, index);
    val suffix = path.substring(index, path.length);
    debug(s"Path: $path, Prefix: $prefix, Suffix: $suffix");
    info(s"Creating $path (is it secure? $isSecure)");
    createRecursive(prefix, suffix);
    val result = waitUntilResolved();
    info(String.format("Result of znode creation is: %s",result))
    result match {
      case Code.OK =>
        // Nothing to do;
      case _ =>
        throw ZkException.create(KeeperException.create(result));
    }
  }
}
