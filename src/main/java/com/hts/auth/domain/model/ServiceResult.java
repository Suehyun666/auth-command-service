package com.hts.auth.domain.model;

import com.hts.generated.grpc.client.*;

public record ServiceResult(AuthResult code, String sessionId, long accountId) {
    public static ServiceResult of(AuthResult code) {
        return new ServiceResult(code, "", 0);
    }

    public static ServiceResult success(String sessionId, long accountId) {
        return new ServiceResult(AuthResult.SUCCESS, sessionId, accountId);
    }

    public static ServiceResult failure(AuthResult code) {
        return new ServiceResult(code, "", 0);
    }

    public boolean isSuccess() {
        return code == AuthResult.SUCCESS;
    }
}
