-- 审计原文保险箱：AES-256 加密后的 SQL 文本，按 TTL + legal_hold 清理。
-- BYTEA 在 H2（PostgreSQL 模式）和 Postgres 都能用；方言保持两者交集。
CREATE TABLE IF NOT EXISTS lake_audit.query_text (
  event_id VARCHAR(64) PRIMARY KEY,
  actor VARCHAR(80) NOT NULL,
  client_ip VARCHAR(64),
  at_ms BIGINT NOT NULL,
  expires_at BIGINT NOT NULL,
  legal_hold BOOLEAN NOT NULL DEFAULT FALSE,
  key_id VARCHAR(32) NOT NULL,
  redacted BOOLEAN NOT NULL,
  nonce BYTEA,
  payload BYTEA
);
