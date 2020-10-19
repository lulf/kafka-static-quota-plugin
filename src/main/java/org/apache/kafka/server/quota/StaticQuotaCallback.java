package org.apache.kafka.server.quota;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 */
public class StaticQuotaCallback implements ClientQuotaCallback {
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        return quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
    }

    @Override
    public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public boolean quotaResetRequired(ClientQuotaType quotaType) {
        return false;
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        log.info("Configured quota callback with {}", quotaMap);
    }
}
