package byteplus.sdk.core.metrics;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static byteplus.sdk.core.metrics.Constant.DEFAULT_METRICS_EXPIRE_TIME_MS;


@Slf4j
public class Counter implements Metrics {

    private final MetricsHttpClient httpCli;

    private final String name;

    private final Map<Item<Double>, MetricRequest<Double>> valueMap;

    private final ConcurrentLinkedQueue<Item<Double>> queue;

    private long expireTime;

    public Counter(String name, int flushTimeMs) {
        this.name = name;
        this.expireTime = System.currentTimeMillis() + DEFAULT_METRICS_EXPIRE_TIME_MS;
        this.httpCli = MetricsHttpClient.getClient(Constant.COUNTER_URL_FORMAT.replace("{}", MetricsConfig.getMetricsDomain()));
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

    @Override
    public void emit(Double value, Map<String, String> tags) {
        TreeMap<String, String> map = new TreeMap<>(tags);
        String tag = MetricsHelper.processTags(map);
        Item<Double> item = new Item<>(tag, value);
        queue.offer(item);
        if (MetricsConfig.isEnablePrintLog()) {
            log.debug("enqueue {} counter success {}", name, item);
        }
    }

    public void flush() {
        try {
            Item<Double> item;
            int size = 0;
            while (size++ < Constant.MAX_FLUSH_SIZE && !queue.isEmpty()) {
                item = queue.poll();
                if (!this.valueMap.containsKey(item)) {
                    MetricRequest<Double> request = new MetricRequest<>();
                    request.setMetric(name);
                    request.setValue(item.getValue());
                    request.setTags(MetricsHelper.recoverTags(item.getTags()));
                    this.valueMap.put(item, request);
                } else {
                    double tmp = this.valueMap.get(item).getValue();
                    tmp += item.getValue();
                    this.valueMap.get(item).setValue(tmp);
                }
            }

            List<MetricRequest> requestList = new ArrayList<>(this.valueMap.values());
            if (!requestList.isEmpty()) {
                long timestamp = System.currentTimeMillis() / 1000L;
                requestList.forEach(key -> {
                    this.valueMap.values().remove(key);
                    if (MetricsConfig.isEnablePrintLog()) {
                        log.info("remove counter key {}", key);
                    }
                });
                for (MetricRequest request : requestList) {
                    request.setTimestamp(timestamp);
                }
                httpCli.put(requestList);
            }
        } catch (Throwable e) {
            log.error("flush counter exception: {} \n {}", e.getMessage(), MetricsHelper.ExceptionUtil.getTrace(e));
        }
    }

}
