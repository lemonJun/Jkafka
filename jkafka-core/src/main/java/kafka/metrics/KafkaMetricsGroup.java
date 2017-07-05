package kafka.metrics;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public abstract class KafkaMetricsGroup {
    /**
     * Creates a new MetricName object for gauges, meters, etc. created for this
     * metrics group.
     *
     * @param name Descriptive name of the metric.
     * @return Sanitized metric name object.
     */
    private MetricName metricName(String name) {
        Class<?> klass = this.getClass();
        String pkg = (klass.getPackage() == null) ? "" : klass.getPackage().getName();
        String simpleName = klass.getSimpleName().replaceAll("\\$$", "");
        return new MetricName(pkg, simpleName, name);
    }

    public <T> Gauge<T> newGauge(String name, Gauge<T> metric) {
        return Metrics.defaultRegistry().newGauge(metricName(name), metric);
    }

    public Meter newMeter(String name, String eventType, TimeUnit timeUnit) {
        return Metrics.defaultRegistry().newMeter(metricName(name), eventType, timeUnit);
    }

    public Histogram newHistogram(String name) {
        return newHistogram(name, true);
    }

    public Histogram newHistogram(String name, boolean biased/* = true*/) {
        return Metrics.defaultRegistry().newHistogram(metricName(name), biased);
    }

    public Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit) {
        return Metrics.defaultRegistry().newTimer(metricName(name), durationUnit, rateUnit);
    }
}
