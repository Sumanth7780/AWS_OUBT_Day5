import os
import io
import json
import sys
import hashlib
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, Json

# ----------------------------
# Config from env (GitHub Actions / local)
# ----------------------------
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY = os.getenv("S3_KEY", "raw_data/reference/taxi_zone_lookup.csv")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

RUN_BY = os.getenv("RUN_BY", "pipeline")

REQUIRED_COLS = ["LocationID", "Zone", "Borough", "service_zone"]

def fail(msg: str):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def connect_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return df

def log_exception(cur, domain, entity_key, rule_name, rule_desc, payload, severity="ERROR"):
    cur.execute(
        """
        INSERT INTO mdm.mdm_exceptions
          (domain, entity_key, rule_name, rule_description, severity, source_file, record_payload, created_by)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (domain, str(entity_key), rule_name, rule_desc, severity, f"s3://{S3_BUCKET}/{S3_KEY}", Json(payload), RUN_BY),
    )

def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    # Basic schema check
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        fail(f"Missing required columns in CSV: {missing}")

    # Standardize
    df = df.copy()
    df["LocationID"] = pd.to_numeric(df["LocationID"], errors="coerce")
    df["Zone"] = df["Zone"].astype(str).str.strip()
    df["Borough"] = df["Borough"].astype(str).str.strip()
    df["service_zone"] = df["service_zone"].astype(str).str.strip()

    # Rule checks
    errors = 0
    bad_rows = []

    # Completeness: LocationID not null
    mask_null_id = df["LocationID"].isna()
    if mask_null_id.any():
        errors += int(mask_null_id.sum())
        bad_rows.append(("NOT_NULL_LOCATIONID", "LocationID must be NOT NULL", df[mask_null_id]))

    # Zone not null/empty
    mask_empty_zone = df["Zone"].isna() | (df["Zone"] == "") | (df["Zone"].str.lower() == "nan")
    if mask_empty_zone.any():
        errors += int(mask_empty_zone.sum())
        bad_rows.append(("NOT_NULL_ZONE", "Zone must be NOT NULL/empty", df[mask_empty_zone]))

    # Uniqueness: LocationID unique
    dup_ids = df[df["LocationID"].notna()].duplicated(subset=["LocationID"], keep=False)
    if dup_ids.any():
        errors += int(dup_ids.sum())
        bad_rows.append(("UNIQUE_LOCATIONID", "LocationID must be UNIQUE", df[dup_ids]))

    return df, errors, bad_rows

def upsert_dim_taxi_zone(cur, df: pd.DataFrame):
    # Map to target columns
    out = df.rename(columns={
        "LocationID": "zone_id",
        "Zone": "zone_name",
        "Borough": "borough",
        "service_zone": "service_zone",
    })[["zone_id", "zone_name", "borough", "service_zone"]].dropna(subset=["zone_id"])

    # Upsert statement
    sql = """
    INSERT INTO mdm.dim_taxi_zone
      (zone_id, zone_name, borough, service_zone, is_active, version, created_by, updated_by, updated_at)
    VALUES %s
    ON CONFLICT (zone_id) DO UPDATE SET
      zone_name = EXCLUDED.zone_name,
      borough = EXCLUDED.borough,
      service_zone = EXCLUDED.service_zone,
      updated_by = EXCLUDED.updated_by,
      updated_at = NOW(),
      version = mdm.dim_taxi_zone.version + 1
    ;
    """

    rows = [
        (int(r.zone_id), r.zone_name, r.borough, r.service_zone, 1, 1, RUN_BY, RUN_BY, datetime.utcnow())
        for r in out.itertuples(index=False)
    ]
    execute_values(cur, sql, rows, page_size=1000)

def main():
    # Guardrails
    for v in [S3_BUCKET, DB_HOST, DB_USER, DB_PASSWORD]:
        if not v:
            fail("Missing required env vars. Need S3_BUCKET, DB_HOST, DB_USER, DB_PASSWORD (and others).")

    print(f"[INFO] Reading taxi zones from s3://{S3_BUCKET}/{S3_KEY}")
    df = read_csv_from_s3(S3_BUCKET, S3_KEY)
    print(f"[INFO] Read {len(df)} rows")

    df2, errors, bad_rows = validate(df)

    conn = connect_db()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Log validation failures to exceptions table
            for rule_name, rule_desc, bad_df in bad_rows:
                for rec in bad_df.head(5000).to_dict(orient="records"):  # safety cap
                    entity_key = rec.get("LocationID", "UNKNOWN")
                    log_exception(cur, "Taxi Zones", entity_key, rule_name, rule_desc, rec)

            # If hard errors exist, stop load (quality gate)
            if errors > 0:
                conn.commit()
                fail(f"Quality gate failed. Logged {errors} exceptions. Master load blocked.")

            # Upsert to master table
            upsert_dim_taxi_zone(cur, df2)
            conn.commit()
            print("[INFO] dim_taxi_zone load successful.")

    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
