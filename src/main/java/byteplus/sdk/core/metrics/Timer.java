package byteplus.sdk.core.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class Timer implements Metric {
    private final MetricsHttpClient httpCli;

    private final String name;

    private final ScheduledExecutorService executor;

    private final Map<String, String> tagMap;

    private final Queue<Long> queue;

    private final Reservoir reservoir;

    private static final ThreadFactory TIMER_THREAD_FACTORY;

    private static final int DEFAULT_FLUSH_TIME = 10 * 1000;

    private final ExpirableMetrics expirableMetrics;

    public boolean isExpired() {
        return this.expirableMetrics.isExpired();
    }

    public Timer updateExpireTime(final long ttlInMs) {
        this.expirableMetrics.updateExpireTime(ttlInMs);
        return this;
    }

    static {
        TIMER_THREAD_FACTORY = new MetricsHelper.NamedThreadFactory("metric-timer-flush");
    }

    public Timer(String name, String tags, Reservoir reservoir) {
        this(name, tags, reservoir, DEFAULT_FLUSH_TIME);
    }

    public Timer(String name, String tags, Reservoir reservoir, int flushTimeMs) {
        this.name = name;
        this.tagMap = MetricsHelper.recoverTags(tags);
        this.reservoir = reservoir;
        this.httpCli = MetricsHttpClient.getClient(Constant.OTHER_URL_FORMAT.replace("{}", MetricsConfig.getMetricsDomain()));
        this.queue = new ConcurrentLinkedQueue<>();
        this.expirableMetrics = new ExpirableMetrics();
        this.executor = Executors.newSingleThreadScheduledExecutor(TIMER_THREAD_FACTORY);
        this.executor.scheduleAtFixedRate(this::flush, 0, flushTimeMs, TimeUnit.MILLISECONDS);
    }

    public void flush() {
        try {
            int size = 0;
            while (size < Constant.MAX_FLUSH_SIZE && !this.queue.isEmpty()) {
                Long item = this.queue.poll();
                this.reservoir.update(item);
                size++;
            }
            Snapshot snapshot = this.reservoir.getSnapshot();
            List<MetricRequest> data = buildMetricList(snapshot, size);
            this.httpCli.emit(data);
            if ((MetricsConfig.isEnablePrintLog())) {
                log.info("remove : {}", data);
            }
        } catch (Throwable e) {
            log.error("flush timer exception: {} \n {}", e.getMessage(), MetricsHelper.ExceptionUtil.getTrace(e));
        }
    }

    public List<MetricRequest> buildMetricList(Snapshot shot, int size) {
        List<MetricRequest> data = new ArrayList<>();
        long timestamp = System.currentTimeMillis() / 1000L;

        //count
        MetricRequest<Long> countRequest = new MetricRequest<>();
        countRequest.setMetric(name + "." + "count");
        countRequest.setTimestamp(timestamp);
        countRequest.setTags(new HashMap<>(this.tagMap));
        countRequest.setValue((long) size);
        data.add(countRequest);

        //max
        MetricRequest<Long> maxRequest = new MetricRequest<>();
        maxRequest.setMetric(name + "." + "max");
        maxRequest.setTimestamp(timestamp);
        maxRequest.setTags(new HashMap<>(this.tagMap));
        maxRequest.setValue(shot.getMax());
        data.add(maxRequest);

        //min
        MetricRequest<Long> minRequest = new MetricRequest<>();
        minRequest.setMetric(name + "." + "min");
        minRequest.setTimestamp(timestamp);
        minRequest.setTags(new HashMap<>(this.tagMap));
        minRequest.setValue(shot.getMin());
        data.add(minRequest);

        //avg
        MetricRequest<Double> avgRequest = new MetricRequest<>();
        avgRequest.setMetric(name + "." + "avg");
        avgRequest.setTimestamp(timestamp);
        avgRequest.setTags(new HashMap<>(this.tagMap));
        avgRequest.setValue(shot.getMean());
        data.add(avgRequest);

        // median
        MetricRequest<Double> medianRequest = new MetricRequest<>();
        medianRequest.setMetric(name + "." + "median");
        medianRequest.setTimestamp(timestamp);
        medianRequest.setTags(new HashMap<>(this.tagMap));
        medianRequest.setValue(shot.getMedian());
        data.add(medianRequest);

        //pc75
        MetricRequest<Double> pc75Request = new MetricRequest<>();
        pc75Request.setMetric(name + "." + "pct75");
        pc75Request.setTimestamp(timestamp);
        pc75Request.setTags(new HashMap<>(this.tagMap));
        pc75Request.setValue(shot.get75thPercentile());
        data.add(pc75Request);

        //pc90
        MetricRequest<Double> pc90Request = new MetricRequest<>();
        pc90Request.setMetric(name + "." + "pct90");
        pc90Request.setTimestamp(timestamp);
        pc90Request.setTags(new HashMap<>(this.tagMap));
        pc90Request.setValue(shot.getValue(0.90D));
        data.add(pc90Request);

        //pc95
        MetricRequest<Double> pc95Request = new MetricRequest<>();
        pc95Request.setMetric(name + "." + "pct95");
        pc95Request.setTimestamp(timestamp);
        pc95Request.setTags(new HashMap<>(this.tagMap));
        pc95Request.setValue(shot.get95thPercentile());
        data.add(pc95Request);

        //pc99
        MetricRequest<Double> pc99Request = new MetricRequest<>();
        pc99Request.setMetric(name + "." + "pct99");
        pc99Request.setTimestamp(timestamp);
        pc99Request.setTags(new HashMap<>(this.tagMap));
        pc99Request.setValue(shot.get99thPercentile());
        data.add(pc99Request);

        //pc999
        MetricRequest<Double> pc999Request = new MetricRequest<>();
        pc999Request.setMetric(name + "." + "pct999");
        pc999Request.setTimestamp(timestamp);
        pc999Request.setTags(new HashMap<>(this.tagMap));
        pc999Request.setValue(shot.get999thPercentile());
        data.add(pc999Request);
        return data;
    }

    public void emit(long value) {
        this.queue.offer(value);
    }

    public void close() {
        this.executor.shutdownNow();
    }

}
