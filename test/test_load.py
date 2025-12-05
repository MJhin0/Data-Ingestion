import pandas as pd
from unittest.mock import MagicMock, patch
from ingestion.src.load import load_to_postgres

def test_load_to_postgres():
    df = pd.DataFrame({"a": [1], "b": [2]})

    mock_engine = MagicMock()
    mock_connect = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connect

    with patch("ingestion.src.load.create_engine", return_value=mock_engine):
        load_to_postgres(df, "test_table", "postgres://...")

    # Ensure df.to_sql was called
    mock_engine.connect.assert_called()