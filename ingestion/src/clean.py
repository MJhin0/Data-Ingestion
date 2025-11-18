import pandas as pd
from pathlib import Path
from ingestion.src.logger import get_logger

logger = get_logger(__name__)


# Standardizes column names: lowercase, strip spaces, replace spaces with underscores
def normalize_columns(df: pd.DataFrame):
    df = df.copy()
    original = df.columns.tolist()
    df.columns = (df.columns.str.strip().str.lower().str.replace(" ", "_"))
    logger.info(f"Normalized columns: {original} -> {df.columns.tolist()}")

    return df


# Trims whitespace and replaces empty strings w/ None for all string/object columns
def clean_strings(df: pd.DataFrame):
    df = df.copy()
    string_cols = df.select_dtypes(include=["object", "string"]).columns

    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"": None, "nan": None, "None": None})

    logger.info(f"Cleaned string columns: {list(string_cols)}")
    return df


# Splits dataframe into valid and rejected, rejected being missing data records
def split_missing(df: pd.DataFrame, required_cols = None):
    if required_cols is None:
        required_cols = df.columns.tolist()

    mask_valid = df[required_cols].notna().all(axis=1)
    valid = df[mask_valid].copy()
    rejected = df[~mask_valid].copy()

    logger.info(f"Rejected {len(rejected)} rows due to missing data")
    rejected["reject_reason"] = "Missing required fields"

    return valid, rejected


# Removes duplicates, returns a clean dataframe and duplicates
def drop_duplicates(df: pd.DataFrame):
    before = len(df)
    df_nodup = df.drop_duplicates(keep = "first")
    after = len(df_nodup)
    removed = before - after

    if removed > 0:
        logger.info(f"Removed {removed} duplicate rows")

    dupes = df[~df.index.isin(df_nodup.index)].copy()
    if len(dupes) > 0:
        dupes["reject_reason"] = "Duplicate row"

    return df_nodup, dupes



def save_to_csv(df: pd.DataFrame, path: str):
    output_path = Path(path)
    output_path.parent.mkdir(parents = True, exist_ok = True)
    df.to_csv(output_path, index = False)
    logger.info(f"Saved: {output_path}")


# The function used in main
def clean(df: pd.DataFrame, source_name: str, required_cols=None):
    df = normalize_columns(df)
    df = clean_strings(df)

    # Missing values
    valid_df, missing_rej = split_missing(df, required_cols = required_cols)
    # Duplicates
    deduped_df, duplicate_rej = drop_duplicates(valid_df)
    # Combine rejects
    rejects_df = pd.concat([missing_rej, duplicate_rej], ignore_index = True)
    # Save cleaned + rejects to CSV
    save_to_csv(deduped_df, f"data/{source_name}_cleaned.csv")
    save_to_csv(rejects_df, f"data/{source_name}_rejected.csv")

    logger.info(
        f"Cleaning complete for {source_name}: "
        f"cleaned={len(deduped_df)}, rejected={len(rejects_df)}"
    )

    return deduped_df, rejects_df