package byteplus.sdk.core.metrics;

import lombok.Getter;

import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class MetricsReporter {

    private boolean enableMetrics;

    private Map<String, String> BASE_TAGS;

    private MetricsManager client;



    @Getter
    public static class ReporterBuilder {
        // if enableMetrics is false, metrics will not report
        private boolean enableMetrics;

        // baseTags will be added with every report
        private Map<String, String> baseTags;

        // prefix of metrics
        private String prefix;

        public ReporterBuilder enableMetrics(boolean enableMetrics) {
            this.enableMetrics = enableMetrics;
            return this;
        }

        public ReporterBuilder baseTags(Map<String, String> baseTags) {
            if (Objects.nonNull(baseTags) && !baseTags.isEmpty()) {
                this.baseTags = baseTags;
            }
            return this;
        }

        public ReporterBuilder prefix(String prefix) {
            if (Objects.nonNull(prefix) && !prefix.equals("")) {
                this.prefix = prefix;
            }
            return this;
        }

        public MetricsReporter build() {
            if (Objects.isNull(baseTags)) {
                baseTags = new TreeMap<>();
            }
            baseTags.put("host", MetricsHelper.LocalHostUtil.getHostAddr());
            if (Objects.isNull(prefix) || prefix.equals("")) {
                prefix = Constant.DEFAULT_METRICS_PREFIX;
            }
            MetricsReporter reporter = new MetricsReporter();
            reporter.BASE_TAGS = baseTags;
            reporter.enableMetrics = enableMetrics;
            reporter.client = MetricsManager.getManager(prefix);
            return reporter;
        }

    }


    /**
     * Counter介绍：https://site.bytedance.net/docs/2080/2717/36906/
     * tags：tag列表，格式"key:value"，通过“:”分割key和value
     * 示例：metrics.Counter("request.count", 1, "method:user")
     */
    public void counter(String key, long value, String... tagKvs) {
        if (!enableMetrics) {
            return;
        }
        client.emitCounter(key, value, MetricsHelper.appendTags(BASE_TAGS, tagKvs));
    }

    /**
     * Timer介绍：https://site.bytedance.net/docs/2080/2717/36907/
     * tags：tag列表，格式"key:value"，通过“:”分割key和value
     * 示例：metrics.Timer("request.cost", 100, "method:user")
     */
    public void timer(String key, long value, String... tagKvs) {
        if (!enableMetrics) {
            return;
        }
        client.emitTimer(key, value, MetricsHelper.appendTags(BASE_TAGS, tagKvs));
    }

    /**
     * Latency介绍：Latency基于timer封装，非Metrics的标准类型。timer介绍：https://site.bytedance.net/docs/2080/2717/36907/
     * tags：tag列表，格式"key:value"，通过“:”分割key和value
     * 示例：metrics.Latency("request.cost", startTime, "method:user")
     */
    public void latency(String key, long begin, String... tagKvs) {
        if (!enableMetrics) {
            return;
        }
        long cost = System.currentTimeMillis() - begin;
        client.emitTimer(key, cost, MetricsHelper.appendTags(BASE_TAGS, tagKvs));
    }

    /**
     * Store介绍：https://site.bytedance.net/docs/2080/2717/36905/
     * tags：tag列表，格式"key:value"，通过“:”分割key和value
     * 示例：metrics.Store("goroutine.count", 400, "ip:127.0.0.1")
     */
    public void store(String key, long value, String... tagKvs) {
        if (!enableMetrics) {
            return;
        }
        client.emitStore(key, value, MetricsHelper.appendTags(BASE_TAGS, tagKvs));
    }

    public void exception(String key, long begin, Exception e, String... tagKvs) {
        String msgTag;
        if (e instanceof SocketTimeoutException && e.getLocalizedMessage().contains("connect timed")) {
            msgTag = "message:connect-timeout";
        } else if (e instanceof SocketTimeoutException && e.getLocalizedMessage().contains("Read timed")) {
            msgTag = "message:read-timeout";
        } else if (e instanceof InterruptedIOException && e.getLocalizedMessage().contains("timeout")) {
            msgTag = "message:timeout";
        } else {
            msgTag = "message:other";
        }
        String[] newTagKvs = new String[tagKvs.length + 1];
        System.arraycopy(tagKvs, 0, newTagKvs, 0, tagKvs.length);
        newTagKvs[tagKvs.length] = msgTag;
        latency(key, begin, newTagKvs);
    }
}
