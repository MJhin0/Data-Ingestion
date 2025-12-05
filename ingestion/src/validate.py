from ingestion.src.logger import get_logger 
import pandas as pd


logger = get_logger(__name__)

# Casts DataFrame columns to the types in the schema
def apply_schema(df: pd.DataFrame, schema: dict):

    df = df.copy()
    rejects = []

    for col, type_name in schema.items():
        if col not in df.columns:
            continue
        try:
            if type_name == "int":
                df[col] = pd.to_numeric(df[col], errors = "coerce").astype("Int64")
            elif type_name == "float":
                df[col] = pd.to_numeric(df[col], errors = "coerce")
            elif type_name == "str":
                df[col] = df[col].astype(str)
            elif type_name == "datetime":
                df[col] = pd.to_datetime(df[col], errors = "coerce")
            else:
                raise ValueError(f"Unsupported type: {type_name}")
        except Exception as e:
            rejects.append({"reason": f"Failed type cast for '{col}'", "raw": df[col].to_dict()})
            logger.error(f"Failed type cast for column {col}: {e}")

    # Rows with NaN in required columns get rejected
    invalid_mask = df.isna().any(axis=1)
    reject_rows = df[invalid_mask]
    valid_rows = df[~invalid_mask]

    for _, row in reject_rows.iterrows():
        rejects.append({"reason": "Type casting failed", "raw": row.to_dict()})

    return valid_rows.reset_index(drop = True), pd.DataFrame(rejects)


# Drops rows where required columns (non-nullable schema fields) are missing.
def enforce_required(df: pd.DataFrame, schema: dict):
    required_cols = list(schema.keys())
    rejects = []

    invalid_mask = df[list(schema.keys())].isna().any(axis = 1)
    invalid_rows = df[invalid_mask]
    valid_rows = df[~invalid_mask]

    for _, row in invalid_rows.iterrows():
        rejects.append({"reason": "Missing required column", "raw": row.to_dict()})

    return valid_rows.reset_index(drop = True), pd.DataFrame(rejects)


# Applies rules from config
def apply_rules(df: pd.DataFrame, rules: list):
    df = df.copy()
    rejects = []

    valid_mask = pd.Series([True] * len(df))

    for r in rules:
        rule = r["rule"]
        try:
            mask = df.eval(rule)
        except Exception:
            logger.error(f"Invalid rule: {rule}")
            continue

        invalid = df[~mask]
        valid_mask &= mask

        for _, row in invalid.iterrows():
            rejects.append({"reason": f"Rule failed: {rule}", "raw": row.to_dict()})

    valid_df = df[valid_mask].reset_index(drop = True)
    reject_df = pd.DataFrame(rejects)

    return valid_df, reject_df


# The function used in main
def validate(df, schema, rules):
    df1, rej1 = apply_schema(df, schema)
    df2, rej2 = enforce_required(df1, schema)
    df3, rej3 = apply_rules(df2, rules)

    rejects = pd.concat([rej1, rej2, rej3], ignore_index = True)
    return df3, rejects