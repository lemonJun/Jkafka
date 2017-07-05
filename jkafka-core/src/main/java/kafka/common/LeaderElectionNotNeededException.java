package kafka.common;

/**
 * This exception is thrown when new leader election is not necessary.
 */
public class LeaderElectionNotNeededException extends KafkaException {
    public LeaderElectionNotNeededException(String format, Object... args) {
        super(format, args);
    }

}
