package byteplus.sdk.core.metrics;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MetricsHttpClient {
    private static final Map<String, MetricsHttpClient> clientCache = new HashMap<>();
    private final static String CONTENT_TYPE_JSON = "application/json";
    private final static int DEFAULT_METRICS_TIMEOUT_MS = 800;
    private final static int DEFAULT_METRICS_RETRY_TIMES = 1;
    private final String url;

    public static MetricsHttpClient getClient(String url) {
        if (clientCache.containsKey(url)) {
            return clientCache.get(url);
        }
        synchronized (MetricsHttpClient.class) {
            if (clientCache.containsKey(url)) {
                return clientCache.get(url);
            }
            MetricsHttpClient client = new MetricsHttpClient(url);
            clientCache.put(url, client);
            return client;
        }
    }

    public MetricsHttpClient(String url){
       this.url = url;
    }

    private static class ClientHolder {
        private static final OkHttpClient client = new OkHttpClient.Builder()
                .protocols(Constant.PROTOCOL_LIST)
                .connectTimeout(DEFAULT_METRICS_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .writeTimeout(DEFAULT_METRICS_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .readTimeout(DEFAULT_METRICS_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(true)
                .build();
        private static OkHttpClient getClient() {
            return ClientHolder.client;
        }
    }

    public boolean send(Request request) {
        boolean result = false;
        Response response = null;
        for (int i = 0; i < DEFAULT_METRICS_RETRY_TIMES; i++) {
            try {
                response = ClientHolder.getClient().newCall(request).execute();
                if (MetricsConfig.isEnablePrintLog()) {
                    log.info("http status code {}", response.code());
                }
                if (response.isSuccessful()) {
                    result = true;
                    break;
                }
            } catch (Throwable e) {
                log.error("emit data exception: {} \n {}", e.getMessage(), MetricsHelper.ExceptionUtil.getTrace(e));
            } finally {
                if (response != null) response.close();
            }
        }
        return result;
    }


    public boolean emit(List<MetricRequest> metricRequests) {
        Request request = buildMetricsRequest(metricRequests);
        return send(request);
    }

    // batch send request
    private Request buildMetricsRequest(List<MetricRequest> listRequest) {
        Request.Builder builder = new Request.Builder();
        for (Map.Entry<String, String> entry : generateMetricsHeader().entrySet()) {
            builder.addHeader(entry.getKey(), entry.getValue());
        }
        builder.url(url);
        RequestBody body = RequestBody.create(JSON.toJSONString(listRequest), MediaType.parse(CONTENT_TYPE_JSON));
        builder.post(body);
        return builder.build();
    }

    private Map<String, String> generateMetricsHeader() {
        Map<String, String> header = new HashMap<>();
        header.put("Content-Type", CONTENT_TYPE_JSON);
        header.put("Accept", CONTENT_TYPE_JSON);
        return header;
    }
}
