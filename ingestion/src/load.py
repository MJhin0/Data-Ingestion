from sqlalchemy import create_engine, text
from ingestion.src.logger import get_logger
import pandas as pd
import uuid


logger = get_logger(__name__)

# Loads the dataframe into PostgreSQL table. Creates table if it doesn't exist, appends data if it does
def load_to_postgres(df: pd.DataFrame, table_name: str, conn_string: str):
    engine = create_engine(conn_string)

    # Adds UUID primary key column if missing
    if "id" not in df.columns:
        df.insert(0, "id", [uuid.uuid4() for _ in range(len(df))])
    
    df.to_sql(table_name, engine, if_exists = "replace", index = False)

    # Adds primary key to table
    with engine.connect() as conn:
        conn.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY (id);'))
        conn.commit()
    logger.info(f"Loaded {len(df)} rows into table {table_name}")

def load_from_config(source_config: dict):
    file_path = source_config['file']
    table_name = source_config['name']
    conn_string = source_config.get('db_url')

    df = pd.read_csv(file_path)
    load_to_postgres(df, table_name, conn_string)