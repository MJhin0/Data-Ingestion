import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_csv(path):

    file_path = Path(path).resolve()

    
    if not file_path.exists():
        logger.error(f"CSV file not found: {path}")
        raise FileNotFoundError(f"CSV file not found: {path}")

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Read CSV: {path}")
    except Exception as e:
        logger.exception(f"Failed to read CSV: {path}")
        raise e
    
    return df

logger.info("CSV reader started")