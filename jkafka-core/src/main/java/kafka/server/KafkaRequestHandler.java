package kafka.server;

public class KafkaRequestHandler implements Runnable {

    private int id;
    private int brokerId;
    private int totalHandlerThreads;
    private Object requestChannel;
    private KafkaApis kafkaApis;

    public KafkaRequestHandler(int id, int brokerId, int totalHandlerThreads, Object requestChannel, KafkaApis kafkaApis) {
        super();
        this.id = id;
        this.brokerId = brokerId;
        this.totalHandlerThreads = totalHandlerThreads;
        this.requestChannel = requestChannel;
        this.kafkaApis = kafkaApis;
    }

    @Override
    public void run() {
        while (true) {

        }
    }

}
