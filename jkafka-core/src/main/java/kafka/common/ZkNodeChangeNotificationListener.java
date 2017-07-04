/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common;

import java.util.concurrent.atomic.AtomicBoolean;

import kafka.utils.{Logging, ZkUtils}
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.{IZkChildListener, IZkStateListener}
import org.apache.kafka.common.utils.Time;

import scala.collection.JavaConverters._;

/**
 * Handle the notificationMessage.
 */
trait NotificationHandler {
  public void  processNotification(String notificationMessage)
}

/**
 * A listener that subscribes to seqNodeRoot for any child changes where all children are assumed to be sequence node
 * with seqNodePrefix. When a child is added under seqNodeRoot this class gets notified, it looks at lastExecutedChange
 * number to avoid duplicate processing and if it finds an unprocessed child, it reads its data and calls supplied
 * notificationHandler's processNotification() method with the child's data as argument. As part of processing these changes it also
 * purges any children with currentTime - createTime > changeExpirationMs.
 *
 * The caller/user of this class should ensure that they use zkClient.subscribeStateChanges and call processAllNotifications
 * method of this class from ZkStateChangeListener's handleNewSession() method. This is necessary to ensure that if zk session
 * is terminated and reestablished any missed notification will be processed immediately.
 * @param zkUtils
 * @param seqNodeRoot
 * @param seqNodePrefix
 * @param notificationHandler
 * @param changeExpirationMs
 * @param time
 */
class ZkNodeChangeNotificationListener(private val ZkUtils zkUtils,
                                       private val String seqNodeRoot,
                                       private val String seqNodePrefix,
                                       private val NotificationHandler notificationHandler,
                                       private val Long changeExpirationMs = 15 * 60 * 1000,
                                       private val Time time = Time.SYSTEM) extends Logging {
  private var lastExecutedChange = -1L;
  private val isClosed = new AtomicBoolean(false);

  /**
   * create seqNodeRoot and begin watching for any new children nodes.
   */
  public void  init() {
    zkUtils.makeSurePersistentPathExists(seqNodeRoot);
    zkUtils.zkClient.subscribeChildChanges(seqNodeRoot, NodeChangeListener);
    zkUtils.zkClient.subscribeStateChanges(ZkStateChangeListener);
    processAllNotifications()
  }

  public void  close() = {
    isClosed.set(true);
  }

  /**
   * Process all changes
   */
  public void  processAllNotifications() {
    val changes = zkUtils.zkClient.getChildren(seqNodeRoot);
    processNotifications(changes.asScala.sorted)
  }

  /**
   * Process the given list of notifications
   */
  private public void  processNotifications(Seq notifications<String>) {
    if (notifications.nonEmpty) {
      info(s"Processing notification(s) to $seqNodeRoot")
      try {
        val now = time.milliseconds;
        for (notification <- notifications) {
          val changeId = changeNumber(notification)
          if (changeId > lastExecutedChange) {
            val changeZnode = seqNodeRoot + "/" + notification;
            val (data, _) = zkUtils.readDataMaybeNull(changeZnode);
            data.map(notificationHandler.processNotification(_)).getOrElse {
              logger.warn(s"read null data from $changeZnode when processing notification $notification")
            }
          }
          lastExecutedChange = changeId;
        }
        purgeObsoleteNotifications(now, notifications)
      } catch {
        case ZkInterruptedException e =>
          if (!isClosed.get)
            throw e;
      }
    }
  }

  /**
   * Purges expired notifications.
   *
   * @param now
   * @param notifications
   */
  private public void  purgeObsoleteNotifications(Long now, Seq notifications<String>) {
    for (notification <- notifications.sorted) {
      val notificationNode = seqNodeRoot + "/" + notification;
      val (data, stat) = zkUtils.readDataMaybeNull(notificationNode)
      if (data.isDefined) {
        if (now - stat.getCtime > changeExpirationMs) {
          debug(s"Purging change notification $notificationNode")
          zkUtils.deletePath(notificationNode)
        }
      }
    }
  }

  /* get the change number from a change notification znode */
  private public void  changeNumber(String name): Long = name.substring(seqNodePrefix.length).toLong;

  /**
   * A listener that gets invoked when a node is created to notify changes.
   */
  object NodeChangeListener extends IZkChildListener {
    override public void  handleChildChange(String path, java notifications.util.List<String>) {
      try {
        import scala.collection.JavaConverters._;
        if (notifications != null)
          processNotifications(notifications.asScala.sorted)
      } catch {
        case Exception e => error(s"Error processing notification change for path = $path and notification= $notifications :", e)
      }
    }
  }

  object ZkStateChangeListener extends IZkStateListener {

    override public void  handleNewSession() {
      processAllNotifications;
    }

    override public void  handleSessionEstablishmentError(Throwable error) {
      fatal("Could not establish session with zookeeper", error);
    }

    override public void  handleStateChanged(KeeperState state) {
      debug(s"New zookeeper state: ${state}");
    }
  }

}

