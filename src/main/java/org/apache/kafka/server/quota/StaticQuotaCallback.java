/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package org.apache.kafka.server.quota;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 */
public class StaticQuotaCallback implements ClientQuotaCallback {
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private final AtomicLong storageUsed = new AtomicLong(0);
    private volatile String logDirs;
    private volatile long storageQuota = Long.MAX_VALUE;
    private volatile int storageCheckInterval = Integer.MAX_VALUE;
    private final StorageChecker storageChecker = new StorageChecker();
    private final Thread storageCheckerThread = new Thread(storageChecker, "storage-quota-checker");

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        // Don't allow producing messages if we're beyond the storage limit.
        if (ClientQuotaType.PRODUCE.equals(quotaType) && storageUsed.get() > storageQuota) {
            log.info("Limiting producer limit because disk is full. Used: {}. Actual: {}", storageUsed.get(), storageQuota);
            return 1.0;
        }
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
        storageQuota = config.getStorageQuota();
        storageCheckInterval = config.getStorageCheckInterval();
        logDirs = config.getLogDirs();

        try {
            storageCheckerThread.interrupt();
            storageCheckerThread.join();
            storageCheckerThread.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        log.info("Configured quota callback with {}. Storage quota: {}. Storage check interval: {}", quotaMap, storageQuota, storageCheckInterval);
    }

    private class StorageChecker implements Runnable {

        @Override
        public void run() {
            if (StaticQuotaCallback.this.logDirs != null && StaticQuotaCallback.this.storageQuota > 0 && StaticQuotaCallback.this.storageCheckInterval > 0) {
                while (true) {
                    try {
                        StaticQuotaCallback.this.storageUsed.set(checkDiskUsage());
                        log.debug("Storage usage checked: {}", StaticQuotaCallback.this.storageUsed.get());
                        Thread.sleep(TimeUnit.SECONDS.toMillis(StaticQuotaCallback.this.storageCheckInterval));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        log.warn("Exception in storage checker thread", e);
                    }
                }

            }
        }

        private long checkDiskUsage() throws IOException {
            try {
                Path folder = Paths.get(logDirs);
                return Files.walk(folder)
                        .filter(p -> p.toFile().isFile())
                        .mapToLong(p -> p.toFile().length())
                        .sum();
            } catch (NoSuchFileException e) {
                return 0;
            }
        }
    }
}
