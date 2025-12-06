package com.hts.auth.domain.service;

import com.hts.auth.domain.model.ServiceResult;
import com.hts.auth.infrastructre.repository.RedisAuthRepository;
import com.hts.generated.grpc.client.*;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class AuthQueryService {

    @Inject RedisAuthRepository redisRepo;

    public Uni<ServiceResult> validateSession(String sessionId) {
        return redisRepo.getSession(sessionId)
                .map(accountId -> {
                    if (accountId == 0L) {
                        return ServiceResult.failure(AuthResult.SESSION_NOT_FOUND);
                    }
                    return ServiceResult.success(sessionId, accountId);
                })
                .onFailure().recoverWithItem(e -> ServiceResult.failure(AuthResult.INTERNAL_ERROR));
    }
}
