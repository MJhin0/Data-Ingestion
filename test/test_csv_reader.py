import pandas as pd
import pytest
from ingestion.src.readers.csv_reader import read_csv
from pathlib import Path

def test_read_csv(tmp_path):
    file = tmp_path / "test.csv"
    file.write_text("a,b\n1,2")

    df = read_csv(str(file))
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1

def test_read_csv_not_found():
    with pytest.raises(FileNotFoundError):
        read_csv("not_real.csv")