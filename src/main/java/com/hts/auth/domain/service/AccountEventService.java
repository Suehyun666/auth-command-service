package com.hts.auth.domain.service;

import com.hts.auth.infrastructre.metrics.DbMetrics;
import com.hts.auth.infrastructre.repository.AuthWriteRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@ApplicationScoped
public class AccountEventService {

    private static final Logger LOG = Logger.getLogger(AccountEventService.class);

    @Inject AuthWriteRepository writeRepo;
    @Inject DbMetrics dbMetrics;

    public Uni<Void> createAccount(long accountId, String password) {
        long start = System.nanoTime();

        return Uni.createFrom().item(() -> {
            boolean created = writeRepo.createAccount(accountId, password);
            if (!created) {
                dbMetrics.incrementFailure("create_account");
                throw new RuntimeException("DB insert failed for account_id=" + accountId);
            }
            dbMetrics.recordWrite(System.nanoTime() - start);
            LOG.infof("Created account for account_id=%d", accountId);
            return null;
        });
    }

    public Uni<Void> deleteAccount(long accountId) {
        long start = System.nanoTime();

        return Uni.createFrom().item(() -> {
            writeRepo.deleteAccount(accountId);
            return accountId;
        })
        .flatMap(aid -> writeRepo.deleteAllSessionsForAccount(aid))
        .invoke(() -> {
            dbMetrics.record("delete_account", "SUCCESS", System.nanoTime() - start);
            LOG.infof("Deleted account and all sessions for account_id=%d", accountId);
        })
        .onFailure().invoke(e -> {
            dbMetrics.record("delete_account", "FAILURE", System.nanoTime() - start);
            LOG.errorf(e, "Exception while deleting account for account_id=%d", accountId);
        });
    }
}
