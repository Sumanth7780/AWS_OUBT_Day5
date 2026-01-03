-- Create schema + core governance tables

CREATE SCHEMA IF NOT EXISTS mdm;

-- Exceptions table (used in Week 2 also)
CREATE TABLE IF NOT EXISTS mdm.mdm_exceptions (
  exception_id      BIGSERIAL PRIMARY KEY,
  domain            VARCHAR(100) NOT NULL,
  entity_key        VARCHAR(200) NOT NULL,
  rule_name         VARCHAR(200) NOT NULL,
  rule_description  TEXT,
  severity          VARCHAR(20) NOT NULL DEFAULT 'ERROR', -- ERROR/WARN
  source_system     VARCHAR(100) DEFAULT 'NYC_TAXI',
  source_file       TEXT,
  record_payload    JSONB,
  status            VARCHAR(20) NOT NULL DEFAULT 'OPEN', -- OPEN/ACK/RESOLVED
  created_at        TIMESTAMP NOT NULL DEFAULT NOW(),
  created_by        VARCHAR(100) NOT NULL DEFAULT 'pipeline',
  resolved_at       TIMESTAMP,
  resolved_by       VARCHAR(100)
);

-- Optional: approvals table (Week 2 approvals workflow)
CREATE TABLE IF NOT EXISTS mdm.mdm_approvals (
  approval_id   BIGSERIAL PRIMARY KEY,
  domain        VARCHAR(100) NOT NULL,
  entity_key    VARCHAR(200) NOT NULL,
  action        VARCHAR(30) NOT NULL, -- CREATE/UPDATE/RETIRE
  requested_by  VARCHAR(100) NOT NULL,
  requested_at  TIMESTAMP NOT NULL DEFAULT NOW(),
  approved_by   VARCHAR(100),
  approved_at   TIMESTAMP,
  status        VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING/APPROVED/REJECTED
  comments      TEXT
);
