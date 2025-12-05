import pandas as pd
from ingestion.src.validate import validate

schema = {
    "a": "int",
    "b": "str"
}

rules = [
    {"rule": "a > 0"}
]


def test_validate_passes_good_data():
    df = pd.DataFrame({"a": [5], "b": ["ok"]})
    valid, reject = validate(df, schema, rules)

    assert len(valid) == 1
    assert len(reject) == 0

def test_validate_rejects_bad_schema():
    df = pd.DataFrame({"a": ["xxx"], "b": ["text"]})
    valid, reject = validate(df, schema, rules)

    assert len(valid) == 0
    assert len(reject) >= 1

def test_validate_rule_fails():
    df = pd.DataFrame({"a": [-3], "b": ["x"]})
    valid, reject = validate(df, schema, rules)

    assert len(valid) == 0
    assert reject.iloc[0]["reason"].startswith("Rule failed")
