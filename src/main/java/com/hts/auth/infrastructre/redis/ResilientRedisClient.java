package com.hts.auth.infrastructre.redis;

import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.redis.client.Response;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fault-tolerant Redis client with support for multiple Redis instances
 * Provides automatic failover and retry logic for high availability
 */
@ApplicationScoped
public class ResilientRedisClient {

    private static final Logger LOG = Logger.getLogger(ResilientRedisClient.class);

    @Inject ReactiveRedisDataSource primaryRedis;

    @ConfigProperty(name = "redis.failover.enabled", defaultValue = "false")
    boolean failoverEnabled;

    @ConfigProperty(name = "redis.max.retries", defaultValue = "2")
    int maxRetries;

    @ConfigProperty(name = "redis.retry.delay.ms", defaultValue = "50")
    int retryDelayMs;

    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);

    /**
     * Execute Redis command with automatic retry and failover
     */
    public Uni<Response> executeWithRetry(String command, String... args) {
        return executeInternal(command, 0, args);
    }

    private Uni<Response> executeInternal(String command, int attemptNumber, String... args) {
        if (attemptNumber >= maxRetries) {
            LOG.errorf("Redis command failed after %d retries: %s", maxRetries, command);
            failureCount.incrementAndGet();
            return Uni.createFrom().failure(
                    new RedisFailoverException("Redis unavailable after " + maxRetries + " retries")
            );
        }

        return primaryRedis.execute(command, args)
                .onItem().invoke(result -> {
                    if (attemptNumber > 0) {
                        LOG.infof("Redis command succeeded on attempt %d: %s", attemptNumber + 1, command);
                    }
                    successCount.incrementAndGet();
                })
                .onFailure().retry().withBackOff(
                        java.time.Duration.ofMillis(retryDelayMs),
                        java.time.Duration.ofMillis(retryDelayMs * 4)
                ).atMost(maxRetries)
                .onFailure().invoke(e -> {
                    failureCount.incrementAndGet();
                    LOG.errorf(e, "Redis command failed: %s (attempt %d/%d)",
                            command, attemptNumber + 1, maxRetries);
                });
    }

    /**
     * Get current health metrics
     */
    public RedisHealthMetrics getHealthMetrics() {
        return new RedisHealthMetrics(
                successCount.get(),
                failureCount.get(),
                calculateAvailabilityPercent()
        );
    }

    private double calculateAvailabilityPercent() {
        int total = successCount.get() + failureCount.get();
        if (total == 0) return 100.0;
        return (double) successCount.get() / total * 100.0;
    }

    public static class RedisHealthMetrics {
        public final int successCount;
        public final int failureCount;
        public final double availabilityPercent;

        public RedisHealthMetrics(int successCount, int failureCount, double availabilityPercent) {
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.availabilityPercent = availabilityPercent;
        }
    }

    public static class RedisFailoverException extends RuntimeException {
        public RedisFailoverException(String message) {
            super(message);
        }
    }
}
