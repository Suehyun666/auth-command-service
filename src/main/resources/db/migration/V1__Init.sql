CREATE TABLE accounts (
    account_id      BIGINT PRIMARY KEY,
    password_hash   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    failed_attempts INT NOT NULL DEFAULT 0,
    locked_until    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE accounts
    ADD CONSTRAINT chk_accounts_status
        CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED', 'LOCKED'));

CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_locked_until ON accounts(locked_until) WHERE locked_until IS NOT NULL;

CREATE TABLE login_history (
    id          BIGSERIAL PRIMARY KEY,
    account_id  BIGINT NOT NULL,
    status      TEXT NOT NULL,
    ip_addr     INET,
    fail_reason TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE login_history
    ADD CONSTRAINT chk_login_history_status
        CHECK (status IN ('SUCCESS', 'FAIL', 'LOCKED'));

CREATE INDEX idx_login_history_account_created ON login_history(account_id, created_at DESC);
CREATE INDEX idx_login_history_created ON login_history(created_at DESC);

-- CREATE TABLE sessions (
--     session_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
--     account_id  BIGINT NOT NULL,
--     ip_addr     INET,
--     created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
--     expired_at  TIMESTAMPTZ,
--     logout_type TEXT
-- );
--
-- ALTER TABLE sessions
--     ADD CONSTRAINT chk_sessions_logout_type
--         CHECK (logout_type IS NULL OR logout_type IN ('MANUAL', 'AUTO_EXPIRE', 'FORCED'));
--
-- CREATE INDEX idx_sessions_account ON sessions(account_id);
-- CREATE INDEX idx_sessions_created ON sessions(created_at DESC);
-- CREATE INDEX idx_sessions_active ON sessions(account_id) WHERE expired_at IS NULL;
