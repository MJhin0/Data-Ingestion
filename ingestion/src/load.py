import pandas as pd
import yaml
from sqlalchemy import create_engine
from ingestion.src.logger import get_logger


logger = get_logger(__name__)

# Loads the dataframe into PostgreSQL table. Creates table if it doesn't exist, appends data if it does
def load_to_postgres(df: pd.DataFrame, table_name: str, conn_string: str):
    engine = create_engine(conn_string)
    df.to_sql(table_name, engine, if_exists = "replace", index = False)
    logger.info(f"Loaded {len(df)} rows into table {table_name}")

def load_from_config(source_config: dict):
    file_path = source_config['file']
    table_name = source_config['name']
    conn_string = source_config.get('db_url')

    df = pd.read_csv(file_path)
    load_to_postgres(df, table_name, conn_string)