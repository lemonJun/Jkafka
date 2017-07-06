package kafka.consumer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;

import com.google.common.collect.Maps;

import kafka.utils.ZkUtils;
import kafka.xend.GuiceDI;

public class WildcardTopicCount extends TopicCount {
    public final String consumerIdString;
    public final TopicFilter topicFilter;
    public final int numStreams;

    public WildcardTopicCount(ZkClient zkClient, String consumerIdString, TopicFilter topicFilter, int numStreams) {
        this.consumerIdString = consumerIdString;
        this.topicFilter = topicFilter;
        this.numStreams = numStreams;
    }

    @Override
    public Map<String, Set<String>> getConsumerThreadIdsPerTopic() {
        List<String> children = GuiceDI.getInstance(ZkUtils.class).getChildrenParentMayNotExist(ZkUtils.BrokerTopicsPath);

        Map<String, Integer> topicCountMap = Maps.newHashMap();
        if (children != null) {
            for (String topic : children) {
                if (topicFilter.isTopicAllowed(topic))
                    topicCountMap.put(topic, numStreams);
            }
        }

        return makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap);
    }

    @Override
    public Map<String, Integer> getTopicCountMap() {
        return null;
    }

    @Override
    public String pattern() {
        return null;
    }
}
