package kafka.server;

public class ServerTest {

    public static void main(String[] args) {
        try {
            KafkaServer server = new KafkaServer();
            server.startup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
