package com.hts.auth.domain.model;

public record AuthReadResult(
    boolean found,
    long accountId,
    String passwordHash,
    String status,
    int failedAttempts,
    Long lockedUntil
) {
    public static AuthReadResult notFound() {
        return new AuthReadResult(false, 0, "", "", 0, null);
    }

    public boolean isLocked() {
        return lockedUntil != null && lockedUntil > System.currentTimeMillis();
    }

    public boolean isActive() {
        return "ACTIVE".equals(status);
    }
}
