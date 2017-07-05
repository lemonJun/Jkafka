package kafka.controller;

import java.util.Set;

import kafka.common.TopicAndPartition;

public interface KafkaControllerMBean {
    Set<TopicAndPartition> shutdownBroker(int id);
}
