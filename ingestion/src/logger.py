import logging
from pathlib import Path

def get_logger(name: str, log_file = "logs/ingestion.log"):
    log_path = Path(log_file)
    # Make log directory if missing
    log_path.parent.mkdir(parents = True, exist_ok = True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent dupe handlers
    if not logger.handlers:
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger