package com.github.lisama569.idempotenteventpipelinejava.service;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 流水线可调参数（application.properties: pipeline.* ）。 */
@Component
@ConfigurationProperties(prefix = "pipeline")
public class PipelineProperties {

    private int maxAttempts = 5;
    private final Backoff backoff = new Backoff();
    private final Dispatcher dispatcher = new Dispatcher();
    private long recoveryLockStaleMillis = 10000;

    public static class Backoff {
        private long baseMillis = 100;
        private double multiplier = 2;

        public long getBaseMillis() {
            return baseMillis;
        }

        public void setBaseMillis(long baseMillis) {
            this.baseMillis = baseMillis;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }
    }

    public static class Dispatcher {
        private boolean enabled = true;
        private long intervalMillis = 200;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getIntervalMillis() {
            return intervalMillis;
        }

        public void setIntervalMillis(long intervalMillis) {
            this.intervalMillis = intervalMillis;
        }
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Backoff getBackoff() {
        return backoff;
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public long getRecoveryLockStaleMillis() {
        return recoveryLockStaleMillis;
    }

    public void setRecoveryLockStaleMillis(long recoveryLockStaleMillis) {
        this.recoveryLockStaleMillis = recoveryLockStaleMillis;
    }
}
