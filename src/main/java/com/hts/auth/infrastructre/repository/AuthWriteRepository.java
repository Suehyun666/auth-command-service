package com.hts.auth.infrastructre.repository;

import com.hts.auth.infrastructre.metrics.DbMetrics;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.jooq.impl.DSL.*;

@ApplicationScoped
public class AuthWriteRepository {

    private static final Logger LOG = Logger.getLogger(AuthWriteRepository.class);

    @Inject DSLContext dsl;
    @Inject DbMetrics metrics;
    @Inject RedisAuthRepository redisRepo;

    private static final String ACCOUNTS = "accounts";
    private static final String LOGIN_HISTORY = "login_history";

    private final ExecutorService asyncExecutor = Executors.newFixedThreadPool(4);

    public boolean createAccount(long accountId, String password) {
        long start = System.nanoTime();
        try {
            int count = dsl.insertInto(table(ACCOUNTS))
                    .set(field("account_id"), accountId)
                    .set(field("password_hash"), password)
                    .set(field("salt"), "")
                    .set(field("status"), "ACTIVE")
                    .set(field("failed_attempts"), 0)
                    .onConflictDoNothing()
                    .execute();

            metrics.record("create_account", count > 0 ? "CREATED" : "ALREADY_EXISTS", System.nanoTime() - start);
            return true;
        } catch (Exception e) {
            metrics.record("create_account", "FAILURE", System.nanoTime() - start);
            LOG.errorf(e, "Failed to create account for account_id=%d", accountId);
            return false;
        }
    }


    public int incrementFailedAttempts(long accountId) {
        long start = System.nanoTime();
        try {
            Record1<Integer> result = dsl.update(table(ACCOUNTS))
                    .set(field("failed_attempts"),
                            field("failed_attempts", Integer.class).plus(1))
                    .where(field("account_id").eq(accountId))
                    .returningResult(field("failed_attempts", Integer.class))
                    .fetchOne();

            metrics.record("increment_failed_attempts", "SUCCESS", System.nanoTime() - start);
            return result != null ? result.value1() : 0;
        } catch (Exception e) {
            metrics.record("increment_failed_attempts", "FAILURE", System.nanoTime() - start);
            LOG.errorf(e, "Failed to increment failed_attempts for account_id=%d", accountId);
            return 0;
        }
    }

    public void lockAccount(long accountId, int failedAttempts, long lockUntilMillis) {
        long start = System.nanoTime();
        try {
            dsl.update(table(ACCOUNTS))
                    .set(field("status"), "LOCKED")
                    .set(field("failed_attempts"), failedAttempts)
                    .set(field("locked_until"), toTimestamp(lockUntilMillis))
                    .where(field("account_id").eq(accountId))
                    .execute();

            metrics.record("lock_account", "SUCCESS", System.nanoTime() - start);
        } catch (Exception e) {
            metrics.record("lock_account", "FAILURE", System.nanoTime() - start);
            LOG.errorf(e, "Failed to lock account_id=%d", accountId);
        }
    }

    private java.sql.Timestamp toTimestamp(long millis) {
        return new java.sql.Timestamp(millis);
    }

    public void resetFailedAttempts(long accountId) {
        long start = System.nanoTime();
        try {
            dsl.update(table(ACCOUNTS))
                    .set(field("failed_attempts"), 0)
                    .where(field("account_id").eq(accountId))
                    .execute();

            metrics.record("reset_failed_attempts", "SUCCESS", System.nanoTime() - start);
        } catch (Exception e) {
            metrics.record("reset_failed_attempts", "FAILURE", System.nanoTime() - start);
            LOG.errorf(e, "Failed to reset failed_attempts for account_id=%d", accountId);
        }
    }

    public void recordLoginHistoryAsync(long accountId, String status, String ip, String reason) {
        asyncExecutor.submit(() -> {
            long start = System.nanoTime();
            try {
                dsl.insertInto(table(LOGIN_HISTORY))
                        .set(field("account_id"), accountId)
                        .set(field("status"), status)
                        .set(field("ip_addr"), field("?::inet", String.class, ip))
                        .set(field("fail_reason"), reason)
                        .execute();

                metrics.record("record_login_history", "SUCCESS", System.nanoTime() - start);
            } catch (Exception e) {
                metrics.record("record_login_history", "FAILURE", System.nanoTime() - start);
                LOG.errorf(e, "Failed to record login_history for account_id=%d", accountId);
            }
        });
    }


    public void deleteAccount(long accountId) {
        long start = System.nanoTime();
        try {
            int count = dsl.deleteFrom(table(ACCOUNTS))
                    .where(field("account_id").eq(accountId))
                    .execute();

            metrics.record("delete_account", count > 0 ? "SUCCESS" : "NOT_FOUND", System.nanoTime() - start);
            if (count > 0) {
                LOG.infof("Deleted account: account_id=%d", accountId);
            } else {
                LOG.warnf("Account not found for deletion: account_id=%d", accountId);
            }
        } catch (Exception e) {
            metrics.record("delete_account", "FAILURE", System.nanoTime() - start);
            LOG.errorf(e, "Failed to delete account: account_id=%d", accountId);
            throw e;
        }
    }

    public void updateAccountStatus(long accountId, String status) {
        long start = System.nanoTime();
        try {
            int count = dsl.update(table(ACCOUNTS))
                    .set(field("status"), status)
                    .where(field("account_id").eq(accountId))
                    .execute();

            if (count > 0) {
                metrics.record("update_account_status", "SUCCESS", System.nanoTime() - start);
                LOG.infof("Updated account status: account_id=%d, status=%s", accountId, status);
            } else {
                metrics.record("update_account_status", "NOT_FOUND", System.nanoTime() - start);
                LOG.warnf("Account not found for status update: account_id=%d", accountId);
            }
        } catch (Exception e) {
            metrics.record("update_account_status", "FAILURE", System.nanoTime() - start);
            LOG.errorf(e, "Failed to update account status: account_id=%d", accountId);
            throw e;
        }
    }

    public Uni<Void> deleteAllSessionsForAccount(long accountId) {
        return redisRepo.deleteAllSessionsForAccount(accountId);
    }
}
