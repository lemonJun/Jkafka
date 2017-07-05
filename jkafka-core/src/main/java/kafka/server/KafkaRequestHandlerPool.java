package kafka.server;

/**
 * 处理request的线程池，请求处理池<--num.io.threads.io线程数量
 * @author baodekang
 *
 */
public class KafkaRequestHandlerPool {

    private int brokerId;
    private Object requestChannel;
    private KafkaApis kafkaApis;
    private int numThreads;

}
