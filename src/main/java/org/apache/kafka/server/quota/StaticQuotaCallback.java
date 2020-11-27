/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package org.apache.kafka.server.quota;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 */
public class StaticQuotaCallback implements ClientQuotaCallback {
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private final AtomicLong storageUsed = new AtomicLong(0);
    private volatile String logDirs;
    private volatile long storageQuotaSoft = Long.MAX_VALUE;
    private volatile long storageQuotaHard = Long.MAX_VALUE;
    private volatile int storageCheckInterval = Integer.MAX_VALUE;
    private final AtomicBoolean resetQuota = new AtomicBoolean(false);
    private final StorageChecker storageChecker = new StorageChecker();

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        // Don't allow producing messages if we're beyond the storage limit.
        long currentStorageUsage = storageUsed.get();
        if (ClientQuotaType.PRODUCE.equals(quotaType) && currentStorageUsage > storageQuotaSoft && currentStorageUsage < storageQuotaHard) {
            double minThrottle = quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
            double limit = minThrottle * (1.0 - (1.0 * (currentStorageUsage - storageQuotaSoft) / (storageQuotaHard - storageQuotaSoft)));
            log.debug("Throttling producer rate because disk is beyond soft limit. Used: {}. Quota: {}", storageUsed, limit);
            return limit;
        } else if (ClientQuotaType.PRODUCE.equals(quotaType) && currentStorageUsage >= storageQuotaHard) {
            log.debug("Limiting producer rate because disk is full. Used: {}. Limit: {}", storageUsed, storageQuotaHard);
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
        return resetQuota.getAndSet(true);
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {
        try {
            storageChecker.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        storageQuotaSoft = config.getSoftStorageQuota();
        storageQuotaHard = config.getHardStorageQuota();
        storageCheckInterval = config.getStorageCheckInterval();
        logDirs = config.getLogDirs();

        storageChecker.start();
        log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}", quotaMap, storageQuotaSoft, storageQuotaHard, storageCheckInterval);
    }

    private class StorageChecker implements Runnable {
        private final Thread storageCheckerThread = new Thread(this, "storage-quota-checker");
        private volatile boolean running = false;

        void start() {
            if (!running) {
                running = true;
                storageCheckerThread.start();
            }
        }

        void stop() throws InterruptedException {
            running = false;
            storageCheckerThread.interrupt();
            storageCheckerThread.join();
        }

        @Override
        public void run() {
            if (StaticQuotaCallback.this.logDirs != null && StaticQuotaCallback.this.storageQuotaSoft > 0 && StaticQuotaCallback.this.storageQuotaHard > 0 && StaticQuotaCallback.this.storageCheckInterval > 0) {
                while (running) {
                    try {
                        long diskUsage = checkDiskUsage();
                        long previousUsage = StaticQuotaCallback.this.storageUsed.getAndSet(diskUsage);
                        if (diskUsage != previousUsage) {
                            StaticQuotaCallback.this.resetQuota.set(true);
                        }
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

        private long checkDiskUsage() throws IOException, InterruptedException {
            String dirs = String.join(" ", logDirs.split(","));
            Process process = Runtime.getRuntime().exec(String.format("du -s -B1 %s", dirs));

            process.waitFor();

            BufferedReader buf = new BufferedReader(new InputStreamReader(process.getInputStream()));
            List<String> stdoutLines = new ArrayList<>();
            String line = "";
            while((line = buf.readLine()) != null) {
                stdoutLines.add(line);
            }

            return stdoutLines.stream().mapToLong(l -> Long.parseLong(l.split("\\s+")[0])).sum();
        }
    }
}
