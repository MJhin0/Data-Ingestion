from ingestion.src.logger import get_logger
from ingestion.src.readers.csv_reader import read_csv
from ingestion.src.clean import clean
from ingestion.src.validate import validate
from ingestion.src.load import load_from_config
from pathlib import Path
import pandas as pd
import yaml
import uuid


logger = get_logger(__name__)

def main():
    logger.info("Starting CSV ingestion")

    # Path relative to project root, can do user input maybe
    csv_path = "data/UberDataset.csv"
    source_name = "UberDataset"

    # Read (puts csv in dataframe)
    df = read_csv(csv_path)
    logger.info(f"Dataframe loaded with {len(df)} rows and {len(df.columns)} columns")

    # Clean (gets rid of missing data/dupes)
    df_clean, rejects_clean = clean(df, source_name)
    logger.info(f"After cleaning: {len(df_clean)} rows valid, {len(rejects_clean)} rejected")

    # Load config frrom yml
    with open("config/sources.yml") as f:
        cfg = yaml.safe_load(f)
    source_cfg = cfg["sources"][0]
    schema = source_cfg["schema"]
    rules = source_cfg["rules"]

    # Validate (apply schemas and rules, save to validated + rejected CSVs)
    df_valid, rejects_valid = validate(df_clean, schema, rules)
    logger.info(f"Validation complete: {len(df_valid)} valid, {len(rejects_valid)} rejected")

    # Save validated + rejects
    save_to_csv(df_valid, f"data/{source_name}_validated.csv")
    all_rejects = pd.concat([rejects_clean, rejects_valid], ignore_index = True)
    save_to_csv(all_rejects, f"data/{source_name}_rejected.csv")
    logger.info(f"Saved CSV files: {source_name}_validated.csv and {source_name}_rejected.csv")
    
    # Load into DBeaver
    cfg["sources"][0]["file"] = f"data/{source_name}_validated.csv"
    load_from_config({**cfg["defaults"], **cfg["sources"][0]})

    print("Pipeline finished.")


# Save parameter dataframe to the parameter path
def save_to_csv(df: pd.DataFrame, path: str):
    output_path = Path(path)
    output_path.parent.mkdir(parents = True, exist_ok = True)
    df.to_csv(output_path, index = False)


if __name__ == "__main__":
    main()