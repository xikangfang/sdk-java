package byteplus.sdk.core.metrics;

import com.codahale.metrics.Metric;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
public class Counter implements Metric {
    static final int DEFAULT_EXPIRE_TIME_MS = 10 * 1000; // 10s

    private final ExpirableMetrics expirableMetrics;

    private final MetricsHttpClient httpCli;

    private final String name;

    private final ScheduledExecutorService executor;

    private final Map<Item<Long>, MetricRequest<Long>> valueMap;

    private final ConcurrentLinkedQueue<Item<Long>> queue;

    public boolean isExpired() {
        return this.expirableMetrics.isExpired();
    }

    public Counter updateExpireTime(final long ttlInMs) {
        this.expirableMetrics.updateExpireTime(ttlInMs);
        return this;
    }

    public Counter(String name) {
        this(name, DEFAULT_EXPIRE_TIME_MS);
    }

    public Counter(String name, int flushTimeMs) {
        this.name = name;
        this.httpCli = MetricsHttpClient.getClient(Constant.COUNTER_URL_FORMAT.replace("{}", MetricsConfig.getMetricsDomain()));
        this.queue = new ConcurrentLinkedQueue<>();
        this.valueMap = new HashMap<>();
        this.expirableMetrics = new ExpirableMetrics();
        this.executor = Executors.newSingleThreadScheduledExecutor(new MetricsHelper.NamedThreadFactory("metric-counter-flush"));
        // should be started after other params set
        this.executor.scheduleAtFixedRate(this::flush, 0, flushTimeMs, TimeUnit.MILLISECONDS);
    }

    public void flush() {
        try {
            Item<Long> item;
            int size = 0;
            while (size++ < Constant.MAX_FLUSH_SIZE && !queue.isEmpty()) {
                item = queue.poll();
                if (!this.valueMap.containsKey(item)) {
                    MetricRequest<Long> request = new MetricRequest<>();
                    request.setMetric(name);
                    request.setValue(item.getValue());
                    request.setTags(MetricsHelper.recoverTags(item.getTags()));
                    this.valueMap.put(item, request);
                } else {
                    long tmp = this.valueMap.get(item).getValue();
                    tmp += item.getValue();
                    this.valueMap.get(item).setValue(tmp);
                }
            }

            List<MetricRequest> requestList = new ArrayList<>(this.valueMap.values());
            if (!requestList.isEmpty()) {
                long timestamp = System.currentTimeMillis() / 1000L;
                requestList.parallelStream().forEach(key -> {
                    this.valueMap.values().remove(key);
                    if (MetricsConfig.isEnablePrintLog()) {
                        log.info("remove counter key {}", key);
                    }
                });
                for (MetricRequest request : requestList) {
                    request.setTimestamp(timestamp);
                }
                boolean success = httpCli.emit(requestList);
                if (!success) {
                    log.error("flush counter fail");
                }
            }
        } catch (Throwable e) {
            log.error("flush counter exception: {} \n {}", e.getMessage(), MetricsHelper.ExceptionUtil.getTrace(e));
        }
    }

    public void emit(long value, Map<String, String> tags) {
        TreeMap<String, String> map = new TreeMap<>(tags);
        String tag = MetricsHelper.processTags(map);
        Item<Long> item = new Item<>(tag, value);
        queue.offer(item);
        if (MetricsConfig.isEnablePrintLog()) {
            log.debug("enqueue {} counter success {}", name, item);
        }
    }

    public void close() {
        this.executor.shutdownNow();
    }

}
