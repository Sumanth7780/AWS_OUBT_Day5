
CREATE SCHEMA IF NOT EXISTS mdm;

-- Current Golden Record table
CREATE TABLE IF NOT EXISTS mdm.dim_taxi_zone (
  zone_id       INT PRIMARY KEY,
  zone_name     VARCHAR(120) NOT NULL,
  borough       VARCHAR(60)  NOT NULL,
  service_zone  VARCHAR(60),

  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  version       INT NOT NULL DEFAULT 1,

  created_by    VARCHAR(80) NOT NULL,
  updated_by    VARCHAR(80) NOT NULL,
  approved_by   VARCHAR(80),

  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- History table (tracks versions)
CREATE TABLE IF NOT EXISTS mdm.dim_taxi_zone_hist (
  hist_id      BIGSERIAL PRIMARY KEY,
  zone_id      INT NOT NULL,
  zone_name    VARCHAR(120) NOT NULL,
  borough      VARCHAR(60)  NOT NULL,
  service_zone VARCHAR(60),

  version      INT NOT NULL,
  is_current   BOOLEAN NOT NULL DEFAULT TRUE,
  valid_from   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  valid_to     TIMESTAMPTZ,

  created_by   VARCHAR(80) NOT NULL,
  approved_by  VARCHAR(80),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_zone_hist_zoneid ON mdm.dim_taxi_zone_hist(zone_id);
CREATE INDEX IF NOT EXISTS idx_zone_hist_current ON mdm.dim_taxi_zone_hist(is_current);

-- Exceptions table (conflicts / invalid / duplicates)
CREATE TABLE IF NOT EXISTS mdm.mdm_exceptions (
  exception_id   BIGSERIAL PRIMARY KEY,
  domain         VARCHAR(50) NOT NULL,
  entity_key     VARCHAR(120) NOT NULL,
  exception_type VARCHAR(50) NOT NULL,  -- INVALID / DUPLICATE / CONFLICT
  details        TEXT NOT NULL,
  severity       VARCHAR(10) NOT NULL DEFAULT 'MED',
  status         VARCHAR(20) NOT NULL DEFAULT 'OPEN',

  created_by     VARCHAR(80) NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_by    VARCHAR(80),
  resolved_at    TIMESTAMPTZ
);
