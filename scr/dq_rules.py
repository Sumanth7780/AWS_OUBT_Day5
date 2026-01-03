import pandas as pd

REQUIRED_COLS = ["LocationID", "Zone", "Borough", "service_zone"]

def run_zone_rules(df: pd.DataFrame):
    """
    Returns:
      passed_df: clean records
      failed_df: records that failed rules
      issues: list[dict] describing rule failures
    """
    issues = []
    failed_mask = pd.Series(False, index=df.index)

    # Schema check
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df2 = df.copy()
    df2["LocationID"] = pd.to_numeric(df2["LocationID"], errors="coerce")
    df2["Zone"] = df2["Zone"].astype(str).str.strip()
    df2["Borough"] = df2["Borough"].astype(str).str.strip()
    df2["service_zone"] = df2["service_zone"].astype(str).str.strip()

    # Rule: LocationID not null
    m = df2["LocationID"].isna()
    if m.any():
        issues.append({"rule": "NOT_NULL_LOCATIONID", "count": int(m.sum())})
        failed_mask |= m

    # Rule: LocationID unique
    m = df2[df2["LocationID"].notna()].duplicated(["LocationID"], keep=False)
    if m.any():
        issues.append({"rule": "UNIQUE_LOCATIONID", "count": int(m.sum())})
        failed_mask |= m

    # Rule: Zone not empty
    m = df2["Zone"].isna() | (df2["Zone"] == "") | (df2["Zone"].str.lower() == "nan")
    if m.any():
        issues.append({"rule": "NOT_NULL_ZONE", "count": int(m.sum())})
        failed_mask |= m

    passed_df = df2.loc[~failed_mask].copy()
    failed_df = df2.loc[failed_mask].copy()
    return passed_df, failed_df, issues
