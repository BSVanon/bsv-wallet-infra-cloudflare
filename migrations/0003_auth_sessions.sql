-- 0003: auth_sessions table for BRC-103/104 session storage.
--
-- Replaces the Workers-KV `AUTH_SESSIONS` namespace previously bound in
-- wrangler.toml. The middleware now uses bsv-auth-cloudflare's
-- `D1SessionStorage` (BSVanon fork, branch `feat/d1-session-storage-and-debounce`)
-- which writes the full `StoredSession` as JSON keyed by session_nonce.
--
-- Schema deliberately stores the whole session struct in one TEXT column —
-- the struct evolves in the middleware crate; carrying it as JSON keeps
-- this migration durable across struct shape changes.
--
-- Indexes:
--   - `idx_auth_sessions_identity` powers `get_session_by_identity` (most-
--     recent session per peer identity key — used during the handshake
--     fast path to reuse an existing valid session).
--   - `idx_auth_sessions_expires` powers eviction sweeps (not yet wired —
--     the consumer can prune `WHERE expires_at_ms < ?` periodically).

CREATE TABLE IF NOT EXISTS auth_sessions (
  session_nonce      TEXT PRIMARY KEY,
  peer_identity_key  TEXT NOT NULL,
  session_json       TEXT NOT NULL,
  last_update_ms     INTEGER NOT NULL,
  expires_at_ms      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_auth_sessions_identity
  ON auth_sessions(peer_identity_key);

CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires
  ON auth_sessions(expires_at_ms);
