package byteplus.sdk.core.metrics;

import lombok.Data;

import java.util.Map;

@Data
public class MetricRequest<T> {
    private String metric;

    private long timestamp;

    private T value;

    private Map<String, String> tags;
}
