package kafka.server;

/**
 * 监听zk session expire，在zk创建broker信息，便于其他broker和consumer获取信息
 * @author baodekang
 *
 */
public class KafkaHealthcheck {

    public void startup() {
        System.out.println("KafkaHealthcheck启动");
    }
}
