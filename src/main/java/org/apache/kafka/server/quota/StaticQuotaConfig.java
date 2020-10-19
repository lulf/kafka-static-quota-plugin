package org.apache.kafka.server.quota;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.Quota;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;

public class StaticQuotaConfig extends AbstractConfig {
    private static final String PRODUCE_QUOTA_PROP = "broker.quota.produce";
    private static final String FETCH_QUOTA_PROP = "broker.quota.fetch";
    private static final String REQUEST_QUOTA_PROP = "broker.quota.request";

    public StaticQuotaConfig(Map<String, ?> props, boolean doLog) {
        super(new ConfigDef()
                        .define(PRODUCE_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Produce bandwidth rate quota (in bytes)")
                        .define(FETCH_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Consume bandwidth rate quota (in bytes)")
                        .define(REQUEST_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Request processing time quota (in seconds)"),
                props,
                doLog);
    }

    Map<ClientQuotaType, Quota> getQuotaMap() {
        Map<ClientQuotaType, Quota> m = new HashMap<>();
        Double produceBound = getDouble(PRODUCE_QUOTA_PROP);
        Double fetchBound = getDouble(FETCH_QUOTA_PROP);
        Double requestBound = getDouble(REQUEST_QUOTA_PROP);

        m.put(ClientQuotaType.PRODUCE, Quota.upperBound(produceBound));
        m.put(ClientQuotaType.FETCH, Quota.upperBound(fetchBound));
        m.put(ClientQuotaType.REQUEST, Quota.upperBound(requestBound));

        return m;
    }
}

