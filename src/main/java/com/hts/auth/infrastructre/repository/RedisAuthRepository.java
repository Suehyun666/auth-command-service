package com.hts.auth.infrastructre.repository;

import com.hts.auth.infrastructre.metrics.RedisMetrics;
import com.hts.auth.infrastructre.redis.ResilientRedisClient;
import io.quarkus.redis.datasource.ReactiveRedisDataSource;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@ApplicationScoped
public class RedisAuthRepository {

    private static final Logger LOG = Logger.getLogger(RedisAuthRepository.class);

    private static final String PREFIX = "session:";
    private static final String ACCT_PREFIX = "acct_sessions:";

    @Inject ReactiveRedisDataSource redis;
    @Inject RedisMetrics metrics;
    @Inject ResilientRedisClient resilientRedis;

    @ConfigProperty(name = "redis.failover.enabled", defaultValue = "false")
    boolean failoverEnabled;

    private String saveScript;
    private String getScript;
    private String deleteScript;
    private String deleteAllScript;

    @PostConstruct
    void init() {
        saveScript = loadLua("lua/save_session.lua");
        getScript = loadLua("lua/get_session.lua");
        deleteScript = loadLua("lua/delete_session.lua");
        deleteAllScript = loadLua("lua/delete_all_sessions.lua");
    }

    private String loadLua(String path) {
        try (var is = getClass().getClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("Lua script not found: " + path);
            }
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Lua script: " + path, e);
        }
    }

    public Uni<String> saveSessionAtomic(long accountId, int ttlSeconds) {
        long start = System.nanoTime();
        String setKey = ACCT_PREFIX + accountId;

        return redis.execute("EVAL", saveScript, "1", setKey,
                String.valueOf(accountId), String.valueOf(ttlSeconds))
                .map(result -> {
                    metrics.recordSet(System.nanoTime() - start);
                    return result != null ? result.toString() : null;
                })
                .onFailure().invoke(e -> {
                    metrics.incrementFailure("save_session");
                    LOG.errorf(e, "Redis saveSessionAtomic failed: account_id=%d", accountId);
                });
    }

    public Uni<Long> getSession(String sessionId) {
        long start = System.nanoTime();
        String sessionKey = PREFIX + sessionId;

        return (failoverEnabled
                ? resilientRedis.executeWithRetry("EVAL", getScript, "1", sessionKey, "1800")
                : redis.execute("EVAL", getScript, "1", sessionKey, "1800"))
                .map(result -> {
                    metrics.recordGet(System.nanoTime() - start);
                    if (result == null) {
                        return 0L;
                    }
                    try {
                        return Long.parseLong(result.toString());
                    } catch (Exception e) {
                        return 0L;
                    }
                })
                .onFailure().invoke(e -> {
                    metrics.recordGet(System.nanoTime() - start);
                    metrics.incrementFailure("get_session");
                    LOG.errorf(e, "Redis getSession failed: session_id=%s", sessionId);
                })
                .onFailure().recoverWithItem(0L);
    }

    public Uni<Void> deleteSession(String sessionId, long accountId) {
        long start = System.nanoTime();
        String sessionKey = PREFIX + sessionId;
        String setKey = ACCT_PREFIX + accountId;

        return redis.execute("EVAL", deleteScript, "2", sessionKey, setKey)
                .invoke(() -> metrics.recordSet(System.nanoTime() - start))
                .onFailure().invoke(e -> {
                    metrics.incrementFailure("delete_session");
                    LOG.errorf(e, "Redis deleteSession failed: session_id=%s, account_id=%d", sessionId, accountId);
                })
                .replaceWithVoid();
    }

    public Uni<Void> deleteAllSessionsForAccount(long accountId) {
        long start = System.nanoTime();
        String setKey = ACCT_PREFIX + accountId;

        return redis.execute("EVAL", deleteAllScript, "1", setKey)
                .invoke(result -> {
                    metrics.recordSet(System.nanoTime() - start);
                    LOG.infof("Deleted %s sessions for account_id=%d", result, accountId);
                })
                .onFailure().invoke(e -> {
                    metrics.incrementFailure("delete_all_sessions");
                    LOG.errorf(e, "Redis deleteAllSessionsForAccount failed: account_id=%d", accountId);
                })
                .replaceWithVoid();
    }
}
