import pandas as pd
from ingestion.src.clean import normalize_columns, clean_strings, drop_duplicates, split_missing


def test_normalize_columns():
    df = pd.DataFrame({"A Col": [1], "B COL ": [2]})
    out = normalize_columns(df)

    assert list(out.columns) == ["a_col", "b_col"]

def test_clean_strings():
    df = pd.DataFrame({"name": ["  John  ", "", None]})
    out = clean_strings(df)

    assert out.loc[0, "name"] == "John"
    assert out.loc[1, "name"] is None or pd.isna(out.loc[1, "name"])
    assert pd.isna(out.loc[2, "name"])

def test_split_missing():
    df = pd.DataFrame({"a": [1, None], "b": ["x", "y"]})

    valid, rejected = split_missing(df, required_cols = ["a", "b"])

    assert len(valid) == 1
    assert len(rejected) == 1
    assert rejected.iloc[0]["reject_reason"] == "Missing required fields"

def test_drop_duplicates():
    df = pd.DataFrame({"a": [1, 1, 2]})
    cleaned, dupes = drop_duplicates(df)

    assert len(cleaned) == 2
    assert len(dupes) == 1
    assert dupes.iloc[0]["reject_reason"] == "Duplicate row"
