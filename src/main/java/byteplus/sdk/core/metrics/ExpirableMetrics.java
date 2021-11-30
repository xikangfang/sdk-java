package byteplus.sdk.core.metrics;


import com.codahale.metrics.Metric;

public class ExpirableMetrics implements Metric {
    public final static long DEFAULT_EXPIRE_TIME_MS = 12 * 60 * 60 * 1000L;

    long expireTime;

    public ExpirableMetrics() {
        this.expireTime = System.currentTimeMillis() + DEFAULT_EXPIRE_TIME_MS;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > this.expireTime;
    }

    public void updateExpireTime(long ttlInMs) {
        if (ttlInMs > 0) {
            this.expireTime = System.currentTimeMillis() + ttlInMs;
        }
    }
}
