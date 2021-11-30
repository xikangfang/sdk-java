package byteplus.sdk.core.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.SlidingWindowReservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class MetricsClient {
    private static final Map<String, MetricsClient> clientCache = new HashMap<>();
    private static final int DEFAULT_TTL_MS = 100 * 1000;
    private static final int DEFAULT_FLUSH_MS = 15 * 1000;

    private final String prefix;
    // expire interval of each counter/timer/store, expired metrics will be cleaned
    private final long ttlInMs;
    //interval of flushing all cache metrics
    private final int flushInterval;

    private final ConcurrentHashMap<String, Store> storeMetrics;

    private final ConcurrentHashMap<String, Counter> counterMetrics;

    private final ConcurrentHashMap<String, Timer> timerMetrics;

    private final ScheduledExecutorService executor;


    public static MetricsClient getClientByPrefix(String prefix) {
        if (clientCache.containsKey(prefix)) {
            return clientCache.get(prefix);
        }
        synchronized (MetricsClient.class) {
            if (clientCache.containsKey(prefix)) {
                return clientCache.get(prefix);
            }
            MetricsClient client = new MetricsClient(prefix, DEFAULT_TTL_MS, DEFAULT_FLUSH_MS);
            clientCache.put(prefix, client);
            return client;
        }
    }

    private MetricsClient() {
        this(Constant.DEFAULT_METRICS_PREFIX, DEFAULT_TTL_MS, DEFAULT_FLUSH_MS);
    }

    public MetricsClient(String prefix, long ttlInMs, int flushInterval) {
        this.prefix = prefix;
        this.ttlInMs = ttlInMs <= 0 ? DEFAULT_TTL_MS : ttlInMs;
        this.flushInterval = flushInterval <= 0 ? DEFAULT_FLUSH_MS : flushInterval;
        this.storeMetrics = new ConcurrentHashMap<>(256);
        this.counterMetrics = new ConcurrentHashMap<>(256);
        this.timerMetrics = new ConcurrentHashMap<>(256);
        this.executor = Executors.newSingleThreadScheduledExecutor(new MetricsHelper.NamedThreadFactory("metric-expire"));
        this.executor.scheduleAtFixedRate(this::tidy, this.ttlInMs, this.ttlInMs, TimeUnit.MILLISECONDS);
    }

    private void tidy() {
        List<String> expiredStores = storeMetrics.entrySet().parallelStream().filter(metrics ->
                metrics.getValue().isExpired()).map(Map.Entry::getKey).collect(Collectors.toList());

        if (!expiredStores.isEmpty()) {
            synchronized (this) {
                expiredStores.parallelStream().forEach(key -> {
                    this.storeMetrics.get(key).close();
                    this.storeMetrics.remove(key);
                    if (MetricsConfig.isEnablePrintLog()) {
                        log.info("remove expired store metrics {}", key);
                    }
                });
            }
        }

        List<String> expiredCounters = counterMetrics.entrySet().parallelStream().filter(metrics ->
                metrics.getValue().isExpired()).map(Map.Entry::getKey).collect(Collectors.toList());

        if (!expiredCounters.isEmpty()) {
            synchronized (this) {
                expiredCounters.parallelStream().forEach(key -> {
                    this.counterMetrics.get(key).close();
                    this.counterMetrics.remove(key);
                    if (MetricsConfig.isEnablePrintLog()) {
                        log.info("remove expired counter metrics {}", key);
                    }
                });
            }
        }

        List<String> expiredTimers = timerMetrics.entrySet().parallelStream().filter(metrics ->
                metrics.getValue().isExpired()).map(Map.Entry::getKey).collect(Collectors.toList());

        if (!expiredTimers.isEmpty()) {
            synchronized (this) {
                expiredTimers.parallelStream().forEach(key -> {
                    this.timerMetrics.get(key).close();
                    this.timerMetrics.remove(key);
                    if (MetricsConfig.isEnablePrintLog()) {
                        log.info("remove expired timer metrics {}", key);
                    }
                });
            }
        }
    }


    public void emitCounter(String name, long value, Map<String, String> tags) {
        getOrAddTsCounter(prefix + "." + name).emit(value, tags);
    }

    public void emitTimer(String name, long value, Map<String, String> tags) {
        this.getOrAddTsTimer(prefix + "." + name, tags).emit(value);
    }

    public void emitStore(String name, double value, Map<String, String> tags) {
        getOrAddTsStore(prefix + "." + name).emit(value, tags);
    }

    private Store getOrAddTsStore(final String name) {
        Store expirableStore = this.storeMetrics.get(name);
        if (expirableStore == null) {
            synchronized (this) {
                if (this.storeMetrics.get(name) == null) {
                    expirableStore = new Store(name, flushInterval)
                            .updateExpireTime(this.ttlInMs);
                    this.storeMetrics.put(name, expirableStore);
                    return expirableStore;
                }
            }
        }
        return this.storeMetrics.get(name).updateExpireTime(this.ttlInMs);
    }

    private Counter getOrAddTsCounter(final String name) {
        Counter expirableCounter = this.counterMetrics.get(name);
        if (expirableCounter == null) {
            synchronized (this) {
                if (this.counterMetrics.get(name) == null) {
                    expirableCounter = new Counter(name, flushInterval)
                            .updateExpireTime(this.ttlInMs);
                    this.counterMetrics.put(name, expirableCounter);
                    return expirableCounter;
                }
            }
        }
        return this.counterMetrics.get(name).updateExpireTime(this.ttlInMs);
    }

    private Timer getOrAddTsTimer(final String name, Map<String, String> tags) {
        String tagString = name;
        String tag = MetricsHelper.processTags(new TreeMap<>(tags));
        if (tags != null) {
            tagString = name + tag;
        }
        Timer expirableTimer = this.timerMetrics.get(tagString);
        if (expirableTimer == null) {
            synchronized (this) {
                if (this.timerMetrics.get(tagString) == null) {
                    expirableTimer = new Timer(name, tag, new LockFreeSlidingWindowReservoir(), flushInterval)
                            .updateExpireTime(this.ttlInMs);
                    this.timerMetrics.put(tagString, expirableTimer);
                    return expirableTimer;
                }
            }
        }
        return this.timerMetrics.get(tagString).updateExpireTime(this.ttlInMs);
    }

    public void close() {
        tidy();
        this.executor.shutdownNow();
    }

    static class LockFreeSlidingWindowReservoir implements Reservoir {
        private final List<Long> measurements;

        private long count;

        private static final int DEFAULT_MAX_WINDOW_SIZE = 65536;

        /**
         * Creates a new {@link SlidingWindowReservoir} which stores the last {@code size} measurements.
         */
        public LockFreeSlidingWindowReservoir() {
            this.measurements = new ArrayList<>();
            this.count = 0L;
        }

        @Override
        public int size() {
            return this.measurements.size();
        }

        @Override
        public void update(long value) {
            int index = (int) (count++ & 0x7FFFFFFFL) % DEFAULT_MAX_WINDOW_SIZE;
            if (this.measurements.size() == DEFAULT_MAX_WINDOW_SIZE) {
                this.measurements.set(index, value);
                return;
            }
            this.measurements.add(value);
        }

        @Override
        public Snapshot getSnapshot() {
            long[] values = new long[measurements.size()];
            synchronized (this) {
                for (int i = 0; i < measurements.size(); i++) {
                    values[i] = measurements.get(i);
                }
                this.measurements.clear(); // 窗口滑动机制
            }

            return new UniformSnapshot(values);
        }
    }

}
