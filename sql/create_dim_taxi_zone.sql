-- Master table (golden records)
CREATE TABLE IF NOT EXISTS mdm.dim_taxi_zone (
  zone_id        INT PRIMARY KEY,
  zone_name      VARCHAR(255) NOT NULL,
  borough        VARCHAR(100) NOT NULL,
  service_zone   VARCHAR(100) NOT NULL,

  -- Governance metadata
  is_active      SMALLINT NOT NULL DEFAULT 1,
  version        INT NOT NULL DEFAULT 1,
  created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMP NOT NULL DEFAULT NOW(),
  created_by     VARCHAR(100) NOT NULL DEFAULT 'mdm_loader',
  updated_by     VARCHAR(100) NOT NULL DEFAULT 'mdm_loader',
  approved_by    VARCHAR(100),
  approved_at    TIMESTAMP
);

-- Week 2: history table for SCD Type 2
CREATE TABLE IF NOT EXISTS mdm.dim_taxi_zone_hist (
  hist_id       BIGSERIAL PRIMARY KEY,
  zone_id       INT NOT NULL,
  zone_name     VARCHAR(255) NOT NULL,
  borough       VARCHAR(100) NOT NULL,
  service_zone  VARCHAR(100) NOT NULL,

  -- SCD2 fields
  effective_from TIMESTAMP NOT NULL,
  effective_to   TIMESTAMP,
  is_current     SMALLINT NOT NULL DEFAULT 1,

  -- Governance
  version        INT NOT NULL,
  change_reason  TEXT,
  changed_by     VARCHAR(100) NOT NULL DEFAULT 'mdm_loader',
  changed_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_zone_hist_zone_id ON mdm.dim_taxi_zone_hist(zone_id);
CREATE INDEX IF NOT EXISTS idx_dim_zone_hist_current ON mdm.dim_taxi_zone_hist(zone_id, is_current);
