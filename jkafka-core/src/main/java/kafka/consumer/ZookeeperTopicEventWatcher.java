package kafka.consumer;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.utils.ZkUtils;
import kafka.xend.GuiceDI;

public class ZookeeperTopicEventWatcher {
    public TopicEventHandler<String> eventHandler;

    public ZookeeperTopicEventWatcher(ZkClient zkClient, TopicEventHandler<String> eventHandler) {
        this.eventHandler = eventHandler;
        startWatchingTopicEvents();
    }

    Logger logger = LoggerFactory.getLogger(ZookeeperTopicEventWatcher.class);
    public Object lock = new Object();

    private void startWatchingTopicEvents() {
        ZkTopicEventListener topicEventListener = new ZkTopicEventListener();
        GuiceDI.getInstance(ZkUtils.class).makeSurePersistentPathExists(ZkUtils.BrokerTopicsPath);

        GuiceDI.getInstance(ZkUtils.class).getZkClient().subscribeStateChanges(new ZkSessionExpireListener(topicEventListener));

        List<String> topics = GuiceDI.getInstance(ZkUtils.class).getZkClient().subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);

        // call to bootstrap topic list
        topicEventListener.handleChildChange(ZkUtils.BrokerTopicsPath, topics);
    }

    private void stopWatchingTopicEvents() {
        GuiceDI.getInstance(ZkUtils.class).getZkClient().unsubscribeAll();
    }

    public void shutdown() {
        synchronized (lock) {
            logger.info("Shutting down topic event watcher.");
            if (GuiceDI.getInstance(ZkUtils.class).getZkClient() != null) {
                stopWatchingTopicEvents();
            } else {
                logger.warn("Cannot shutdown since the embedded zookeeper client has already closed.");
            }
        }
    }

    class ZkTopicEventListener implements IZkChildListener {

        @Override
        public void handleChildChange(String s, List<String> strings) {
            synchronized (lock) {
                try {
                    if (GuiceDI.getInstance(ZkUtils.class).getZkClient() != null) {
                        List<String> latestTopics = GuiceDI.getInstance(ZkUtils.class).getZkClient().getChildren(ZkUtils.BrokerTopicsPath);
                        logger.debug("all topics: {}", latestTopics);
                        eventHandler.handleTopicEvent(latestTopics);
                    }
                } catch (Throwable e) {
                    logger.error("error in handling child changes", e);
                }
            }
        }
    }

    class ZkSessionExpireListener implements IZkStateListener {
        public ZkTopicEventListener topicEventListener;

        ZkSessionExpireListener(ZkTopicEventListener topicEventListener) {
            this.topicEventListener = topicEventListener;
        }

        @Override
        public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {

        }

        @Override
        public void handleNewSession() throws Exception {
            synchronized (lock) {
                if (GuiceDI.getInstance(ZkUtils.class).getZkClient() != null) {
                    logger.info("ZK expired: resubscribing topic event listener to topic registry");
                    GuiceDI.getInstance(ZkUtils.class).getZkClient().subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);
                }
            }
        }

        public void handleSessionEstablishmentError(Throwable throwable) throws Exception {

        }
    }

}
