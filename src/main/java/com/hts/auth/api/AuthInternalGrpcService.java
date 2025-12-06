package com.hts.auth.api;

import com.hts.auth.domain.model.ServiceResult;
import com.hts.auth.domain.service.AuthQueryService;
import com.hts.generated.grpc.internal.AuthInternalService;
import com.hts.generated.grpc.internal.ValidateSessionReply;
import com.hts.generated.grpc.internal.ValidateSessionRequest;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

@GrpcService
public class AuthInternalGrpcService implements AuthInternalService {

    @Inject AuthQueryService queryService;

    @Override
    public Uni<ValidateSessionReply> validateSession(ValidateSessionRequest request) {
        return queryService.validateSession(request.getSessionId())
                .map(this::toValidateSessionReply);
    }

    private ValidateSessionReply toValidateSessionReply(ServiceResult result) {
        return ValidateSessionReply.newBuilder()
                .setIsValid(result.isSuccess())
                .setAccountId(result.accountId())
                .build();
    }
}
