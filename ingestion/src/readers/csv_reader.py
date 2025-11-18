import pandas as pd
from pathlib import Path
from ingestion.src.logger import get_logger

# logging.basicConfig(level=logging.INFO)
logger = get_logger(__name__)

def read_csv(path: str):

    file_path = Path(path).resolve()
    
    # Checking for CSV file requested
    if not file_path.exists():
        logger.error(f"CSV file not found: {path}")
        raise FileNotFoundError(f"CSV file not found: {path}")

    # Reading CSV
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Read CSV: {path}")
    except Exception as e:
        logger.exception(f"Failed to read CSV: {path}")
        raise e
    
    # df.columns = df.columns.str.strip()
    return df