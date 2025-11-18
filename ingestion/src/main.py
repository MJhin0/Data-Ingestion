from ingestion.src.logger import get_logger
from ingestion.src.readers.csv_reader import read_csv

logger = get_logger(__name__)

def main():
    logger.info("Starting CSV ingestion")

    # Path relative to project root, can do user input maybe
    csv_path = "data/UberDataset.csv"
    df = read_csv(csv_path)
    logger.info(f"Dataframe loaded with {len(df)} rows and {len(df.columns)} columns")
    print(df.head()) # show output


'''
    schema = {
        "customer_id": "int",
        "first_name": "str",
        "last_name": "str",
        "email": "str",
        "created_at": "datetime"
    }   

    rules = [
        {"rule": "email.str.contains('@')"},
        {"rule": "first_name != ''"}
    ]

    df = read_csv("data/customers.csv")
    valid, rejects = validate(df, schema, rules)

    print("VALID ROWS:")
    print(valid)

    print("REJECTED:")
    print(rejects)
'''


if __name__ == "__main__":
    main()