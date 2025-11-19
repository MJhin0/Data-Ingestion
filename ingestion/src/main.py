from ingestion.src.logger import get_logger
from ingestion.src.readers.csv_reader import read_csv
from ingestion.src.clean import clean
from ingestion.src.validate import validate
from pathlib import Path
import pandas as pd
import os


logger = get_logger(__name__)

def main():
    logger.info("Starting CSV ingestion")

    # Path relative to project root, can do user input maybe
    csv_path = "data/UberDataset.csv"
    source_name = "UberDataset"

    # source_name = os.path.splitext(os.path.basename(csv_path))[0]

    # Read (puts csv in df)
    df = read_csv(csv_path)
    logger.info(f"Dataframe loaded with {len(df)} rows and {len(df.columns)} columns")

    # Clean (gets rid of missing data/dupes)
    df_clean, rejects_clean = clean(df, source_name)
    logger.info(f"After cleaning: {len(df_clean)} rows valid, {len(rejects_clean)} rejected")

    # Validate (apply schemas and rules, save to validated + rejected CSVs)
    schema = {
        "start_date": "datetime",           # maybe change
        "end_date": "datetime",             # maybe change
        "category": "str",
        "start": "str",
        "stop": "str",
        "miles": "float",
        "purpose": "str"
    }
    rules = [
        {"rule": "miles >= 0"},                     # cannot be negative
        {"rule": "start_date <= end_date"}          # trip must end after it starts
#       {"rule": "category in ['?????', '?????']"}  # if want to get specific categories
#       {"rule": "len(start) > 0"}                  # can't be blank, redundant with cleaning
#       {"rule": "len(stop) > 0"}                   # can't be blank, redundant with cleaning
#       {"rule": "len(purpose) > 0"}                # can't be blank, redundant with cleaning
    ]
    df_valid, rejects_validate = validate(df_clean, schema, rules, source_name)
    logger.info(f"Validation complete: {len(df_valid)} valid, {len(rejects_validate)} rejected")

    # Save validated + rejects
    save_to_csv(df_valid, f"data/{source_name}_validated.csv")
    all_rejects = pd.concat([rejects_clean, rejects_validate], ignore_index = True)
    save_to_csv(all_rejects, f"data/{source_name}_rejected.csv")
    logger.info(f"Saved CSV files: {source_name}_validated.csv and {source_name}_rejected.csv")

    print("Pipeline finished.")


# Save parameter df to the parameter path
def save_to_csv(df: pd.DataFrame, path: str):
    output_path = Path(path)
    output_path.parent.mkdir(parents = True, exist_ok = True)
    df.to_csv(output_path, index = False)


if __name__ == "__main__":
    main()