package byteplus.sdk.core.metrics;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Store {
    static final int DEFAULT_EXPIRE_TIME_MS = 10 * 1000; // 10s

    private final MetricsHttpClient httpCli;

    private final String name;

    private final ScheduledExecutorService executor;

    private final Map<Item<Double>, MetricRequest<Double>> valueMap;

    private final ConcurrentLinkedQueue<Item<Double>> queue;


    private final ExpirableMetrics expirableMetrics;

    public boolean isExpired() {
        return this.expirableMetrics.isExpired();
    }

    public Store updateExpireTime(final long ttlInMs) {
        this.expirableMetrics.updateExpireTime(ttlInMs);
        return this;
    }


    public Store(String name) {
        this(name, DEFAULT_EXPIRE_TIME_MS);
    }

    public Store(String name, int flushTimeMs) {
        this.httpCli = MetricsHttpClient.getClient(Constant.OTHER_URL_FORMAT.replace("{}", MetricsConfig.getMetricsDomain()));
        this.name = name;
        this.queue = new ConcurrentLinkedQueue<>();
        this.valueMap = new HashMap<>();
        this.expirableMetrics = new ExpirableMetrics();
        this.executor = Executors.newSingleThreadScheduledExecutor(new MetricsHelper.NamedThreadFactory("metric-store-flush"));
        this.executor.scheduleAtFixedRate(this::flush, 0, flushTimeMs, TimeUnit.MILLISECONDS);
    }

    public void flush() {
        try {
            int size = 0;
            while (size++ < Constant.MAX_FLUSH_SIZE && !this.queue.isEmpty()) {
                Item<Double> item = queue.poll();
                if (valueMap.containsKey(item)) {
                    valueMap.get(item).setValue(item.getValue());
                } else {
                    MetricRequest<Double> request = new MetricRequest<>();
                    request.setMetric(name);
                    request.setValue(item.getValue());
                    request.setTags(MetricsHelper.recoverTags(item.getTags()));
                    valueMap.put(item, request);
                }
            }

            List<MetricRequest> requestList = new ArrayList<>(this.valueMap.values());
            if (!requestList.isEmpty()) {
                long timestamp = System.currentTimeMillis() / 1000L;
                requestList.parallelStream().forEach(key -> {
                    this.valueMap.values().remove(key);
                    if ((MetricsConfig.isEnablePrintLog())) {
                        log.info("remove store key {}", key);
                    }
                });
                for (MetricRequest request : requestList) {
                    request.setTimestamp(timestamp);
                }
                boolean result = httpCli.emit(requestList);
                if (!result) {
                    log.error("flush store fail");
                }
            }
        } catch (Exception e) {
            log.error("flush store exception: {} \n {}", e.getMessage(), MetricsHelper.ExceptionUtil.getTrace(e));
        }

    }

    public void emit(double value, Map<String, String> tags) {
        TreeMap<String, String> map = new TreeMap<>(tags);
        String tag = MetricsHelper.processTags(map);
        Item<Double> item = new Item<>(tag, value);
        queue.offer(item);
        if (MetricsConfig.isEnablePrintLog()) {
            log.debug("enqueue {} store success {}", name, item);
        }
    }

    public void close() {
        this.executor.shutdownNow();
    }

}