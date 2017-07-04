package kafka.consumer;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class TopicCount {
    public abstract Map<String, Set<String>> getConsumerThreadIdsPerTopic();

    public abstract Map<String, Integer> getTopicCountMap();

    public abstract String pattern();

    protected Map<String, Set<String>> makeConsumerThreadIdsPerTopic(String consumerIdString, Map<String, Integer> topicCountMap) {
        Map<String, Set<String>> consumerThreadIdsPerTopicMap = Maps.newHashMap();
        for (Map.Entry<String, Integer> entry : topicCountMap.entrySet()) {
            String topic = entry.getKey();
            Integer nConsumers = entry.getValue();
            Set<String> consumerSet = Sets.newTreeSet();
            for (int i = 0; i < nConsumers; ++i) {
                consumerSet.add(consumerIdString + "-" + i);
            }

            consumerThreadIdsPerTopicMap.put(topic, consumerSet);
        }

        return consumerThreadIdsPerTopicMap;
    }

    public static TopicCount parse(String consumerIdString, String jsonString) {
        try {
            Map<String, Integer> topicCountMap = mapper.readValue(jsonString, new TypeReference<Map<String, Integer>>() {
            });
            return new TopicCount(consumerIdString, topicCountMap);
        } catch (Exception e) {
            throw new IllegalArgumentException("error parse consumer json string " + jsonString, e);
        }
    }

}
