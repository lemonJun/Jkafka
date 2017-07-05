package kafka.server;

import java.util.Properties;

/**
 * kafka核心入口类
 * @author baodekang
 *
 */
public class KafkaServerStartable {

    private KafkaServer kafkaServer;

    public KafkaServerStartable(KafkaConfig serverConfig) {
        this.kafkaServer = new KafkaServer(serverConfig);
    }

    public static KafkaServerStartable fromProps(Properties serverProps) {
        return new KafkaServerStartable(KafkaConfig.fromProps(serverProps));
    }

    public void startup() {
        kafkaServer.startup();
    }

    public void shutdown() {
        kafkaServer.shutdown();
    }

    public void awaitShutdown() {
        kafkaServer.awaitShutdown();
    }
}
