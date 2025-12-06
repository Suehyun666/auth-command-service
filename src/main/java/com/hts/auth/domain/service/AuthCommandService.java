package com.hts.auth.domain.service;

import com.hts.auth.domain.model.AuthReadResult;
import com.hts.auth.domain.model.ServiceResult;
import com.hts.auth.infrastructre.metrics.CommandMetrics;
import com.hts.auth.infrastructre.repository.AuthReadRepository;
import com.hts.auth.infrastructre.repository.AuthWriteRepository;
import com.hts.auth.infrastructre.repository.RedisAuthRepository;
import com.hts.generated.grpc.client.*;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@ApplicationScoped
public class AuthCommandService {

    private static final Logger LOG = Logger.getLogger(AuthCommandService.class);

    @Inject AuthReadRepository readRepo;
    @Inject AuthWriteRepository writeRepo;
    @Inject RedisAuthRepository redisRepo;
    @Inject CommandMetrics commandMetrics;

    private static final int MAX_FAILED_ATTEMPTS = 5;
    private static final long LOCK_DURATION_MILLIS = 30 * 60 * 1000L;
    private static final int SESSION_TTL_SECONDS = 1800;

    /**
     * 로그인: account 조회 → 상태 체크 → 패스워드 검증 → 세션 생성(Redis) → login_history 비동기 기록
     */
    public Uni<ServiceResult> login(long accountId, String password, String ip) {
        long start = System.nanoTime();

        return Uni.createFrom().item(() -> readRepo.findByAccountId(accountId))
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .flatMap(account -> {
                    if (!account.found()) {
                        writeRepo.recordLoginHistoryAsync(accountId, "FAIL", ip, "ACCOUNT_NOT_FOUND");
                        return Uni.createFrom().item(ServiceResult.failure(AuthResult.ACCOUNT_NOT_FOUND));
                    }

                    if (account.isLocked()) {
                        writeRepo.recordLoginHistoryAsync(accountId, "LOCKED", ip, "ACCOUNT_LOCKED");
                        return Uni.createFrom().item(ServiceResult.failure(AuthResult.ACCOUNT_LOCKED));
                    }

                    if (!account.isActive()) {
                        writeRepo.recordLoginHistoryAsync(accountId, "FAIL", ip, "ACCOUNT_SUSPENDED");
                        return Uni.createFrom().item(ServiceResult.failure(AuthResult.ACCOUNT_SUSPENDED));
                    }

                    // 암호화 검증
                    // boolean valid = PasswordHasher.verify(password, account.passwordHash());
                    boolean valid = password.equals(account.passwordHash());
                    if (!valid) {
                        return handleLoginFailure(account,ip);
                    }
                    return handleLoginSuccess(accountId, ip);
                })
                .onItem().invoke(result ->
                        commandMetrics.record("LOGIN", result.code().name(), System.nanoTime() - start));
    }

    private Uni<ServiceResult> handleLoginFailure(AuthReadResult account, String ip) {
        long accountId = account.accountId();

        return Uni.createFrom().item(() -> {
                    int newFailed = writeRepo.incrementFailedAttempts(accountId);
                    if (newFailed >= MAX_FAILED_ATTEMPTS) {
                        long lockUntil = System.currentTimeMillis() + LOCK_DURATION_MILLIS;
                        writeRepo.lockAccount(accountId, newFailed, lockUntil);
                        writeRepo.recordLoginHistoryAsync(accountId, "LOCKED", ip, "MAX_ATTEMPTS_EXCEEDED");
                        return ServiceResult.failure(AuthResult.ACCOUNT_LOCKED);
                    }

                    writeRepo.recordLoginHistoryAsync(accountId, "FAIL", ip, "INVALID_PASSWORD");
                    return ServiceResult.failure(AuthResult.INVALID_CREDENTIALS);
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private Uni<ServiceResult> handleLoginSuccess(long accountId, String ip) {
        return Uni.createFrom().item(() -> {
                    writeRepo.resetFailedAttempts(accountId);
                    return accountId;
                })
                .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .flatMap(aid -> redisRepo.saveSessionAtomic(aid, SESSION_TTL_SECONDS)
                        .map(sessionId -> ServiceResult.success(sessionId, aid)))
                .invoke(() -> writeRepo.recordLoginHistoryAsync(accountId, "SUCCESS", ip, null));
    }

    public Uni<ServiceResult> logout(String sessionId, long accountId) {
        long start = System.nanoTime();
        return redisRepo.deleteSession(sessionId, accountId)
                .replaceWith(ServiceResult.of(AuthResult.SUCCESS))
                .onFailure().recoverWithItem(e -> {
                    LOG.errorf(e, "Logout failed for session_id=%s", sessionId);
                    return ServiceResult.failure(AuthResult.INTERNAL_ERROR);
                })
                .onItem().invoke(result ->
                        commandMetrics.record("LOGOUT", result.code().name(), System.nanoTime() - start));
    }

}
