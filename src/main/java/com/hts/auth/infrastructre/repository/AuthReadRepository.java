package com.hts.auth.infrastructre.repository;

import com.hts.auth.domain.model.AuthReadResult;
import com.hts.auth.infrastructre.metrics.DbMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.Record;

import java.sql.Timestamp;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@ApplicationScoped
public class AuthReadRepository {

    @Inject DSLContext dsl;
    @Inject DbMetrics metrics;

    private static final String ACCOUNTS = "accounts";

    public AuthReadResult findByAccountId(long accountId) {
        long start = System.nanoTime();
        try {
            Record record = dsl.select(
                            field("account_id", Long.class),
                            field("password_hash", String.class),
                            field("status", String.class),
                            field("failed_attempts", Integer.class),
                            field("locked_until", Timestamp.class)
                    )
                    .from(table(ACCOUNTS))
                    .where(field("account_id").eq(accountId))
                    .fetchOne();

            if (record == null) {
                metrics.record("find_by_account_id", "NOT_FOUND", System.nanoTime() - start);
                return AuthReadResult.notFound();
            }

            metrics.record("find_by_account_id", "SUCCESS", System.nanoTime() - start);

            Timestamp lockedUntilTs = record.get(field("locked_until", Timestamp.class));
            Long lockedUntil = lockedUntilTs != null ? lockedUntilTs.getTime() : null;

            return new AuthReadResult(
                    true,
                    record.get(field("account_id", Long.class)),
                    record.get(field("password_hash", String.class)),
                    record.get(field("status", String.class)),
                    record.get(field("failed_attempts", Integer.class)),
                    lockedUntil
            );
        } catch (Exception e) {
            metrics.record("find_by_account_id", "FAILURE", System.nanoTime() - start);
            throw e;
        }
    }
}
