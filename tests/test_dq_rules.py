import pandas as pd
from src.dq_rules import run_zone_rules

def test_zone_rules_detects_null_locationid():
    df = pd.DataFrame({
        "LocationID": [1, None],
        "Zone": ["A", "B"],
        "Borough": ["Manhattan", "Queens"],
        "service_zone": ["Yellow Zone", "Boro Zone"],
    })
    passed, failed, issues = run_zone_rules(df)
    assert len(failed) == 1
    assert any(i["rule"] == "NOT_NULL_LOCATIONID" for i in issues)

def test_zone_rules_detects_duplicates():
    df = pd.DataFrame({
        "LocationID": [1, 1],
        "Zone": ["A", "A2"],
        "Borough": ["Manhattan", "Manhattan"],
        "service_zone": ["Yellow Zone", "Yellow Zone"],
    })
    passed, failed, issues = run_zone_rules(df)
    assert len(failed) == 2
    assert any(i["rule"] == "UNIQUE_LOCATIONID" for i in issues)
