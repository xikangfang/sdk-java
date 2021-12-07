package byteplus.sdk.core.metrics;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static byteplus.sdk.core.metrics.Constant.DEFAULT_METRICS_EXPIRE_TIME_MS;

@Slf4j
public class Store implements Metrics {
    private final MetricsHttpClient httpCli;

    private final String name;

    private long expireTime;

    private final Map<Item<Double>, MetricRequest<Double>> valueMap;

    private final ConcurrentLinkedQueue<Item<Double>> queue;

    public Store(String name, int flushTimeMs) {
        this.httpCli = MetricsHttpClient.getClient(Constant.OTHER_URL_FORMAT.replace("{}", MetricsConfig.getMetricsDomain()));
        this.name = name;
        this.expireTime = System.currentTimeMillis() + DEFAULT_METRICS_EXPIRE_TIME_MS;
        this.queue = new ConcurrentLinkedQueue<>();
        this.valueMap = new HashMap<>();
    }

    public String getName() {
        return this.name;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > this.expireTime;
    }

    public void updateExpireTime(long ttlInMs) {
        if (ttlInMs > 0) {
            this.expireTime = System.currentTimeMillis() + ttlInMs;
        }
    }

    public void emit(Double value, Map<String, String> tags) {
        TreeMap<String, String> map = new TreeMap<>(tags);
        String tag = MetricsHelper.processTags(map);
        Item<Double> item = new Item<>(tag, value);
        queue.offer(item);
        if (MetricsConfig.isEnablePrintLog()) {
            log.debug("enqueue {} store success {}", name, item);
        }
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
                requestList.forEach(key -> {
                    this.valueMap.values().remove(key);
                    if ((MetricsConfig.isEnablePrintLog())) {
                        log.info("remove store key {}", key);
                    }
                });
                for (MetricRequest request : requestList) {
                    request.setTimestamp(timestamp);
                }
                httpCli.put(requestList);
            }
        } catch (Exception e) {
            log.error("flush store exception: {} \n {}", e.getMessage(), MetricsHelper.ExceptionUtil.getTrace(e));
        }

    }

}