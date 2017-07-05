package kafka.log;

import java.util.concurrent.TimeUnit;

import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;

public class LogFlushStats extends KafkaMetricsGroup {
    public static final LogFlushStats instance = new LogFlushStats();

    public KafkaTimer logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
}
