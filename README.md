# Data Ingestion Pipeline

A modular Python pipeline for reading, cleaning, validating, and loading
CSV data into PostgreSQL.\
Includes automated testing with **pytest**, YAML-based configuration,
and UUID-based primary key creation.

------------------------------------------------------------------------

## Features

-   **CSV Reading** using a dedicated reader module
-   **Cleaning**: column normalization, string cleanup, removing duplicates, splitting missing rows
-   **Validation** using schema + rule checks from YAML
-   **Loading** to PostgreSQL using SQLAlchemy
-   **UUID Primary Keys** automatically generated during load
-   **Logging** to `logs/ingestion.log`
-   **Unit Tests** for every stage (`clean`, `read_csv`, `validate`,
    `load`)
-   **Config-driven ingestion** via `sources.yml`

------------------------------------------------------------------------

## Project Structure

    data-ingestion/
    │
    ├── config/
    │   └── sources.yml
    │
    ├── data/
    │   └── UberDataset_rejected.csv
    │   └── UberDataset_validated.csv
    │   └── UberDataset.csv
    │
    ├── ingestion/
    │   └── src/
    │       ├── readers/
    │       │   ├── __init__.py
    │       │   └── csv_reader.py
    │       ├── clean.py
    │       ├── load.py
    │       ├── logger.py
    │       ├── main.py
    │       ├── validate.py
    │       └── __init__.py
    │
    ├── logs/
    │   └── ingestion.log
    │
    ├── test/
    │   ├── test_clean.py
    │   ├── test_csv_reader.py
    │   ├── test_load.py
    │   └── test_validate.py
    │
    ├── pytest.ini
    ├── requirements.txt
    └── README.md

------------------------------------------------------------------------

## Running the Pipeline

``` bash
python -m ingestion.src.main
```

This will:
1.  Read the CSV
2.  Clean it
3.  Validate using YAML config
4.  Save `*_validated.csv` and `*_rejected.csv`
5.  Insert validated data into PostgreSQL
6.  Auto-generate UUID primary keys

------------------------------------------------------------------------

## Running Tests

Pytest is preconfigured with `pytest.ini`.
Run all tests:

``` bash
pytest -v
```

------------------------------------------------------------------------

## Database Load Logic

The loader:
-   Creates a SQLAlchemy engine
-   Loads DataFrame via `df.to_sql(...)`
-   Adds a UUID column before loading
-   Ensures a primary key exists

------------------------------------------------------------------------

## Logging

All logs go to:
    logs/ingestion.log


------------------------------------------------------------------------

Source of dataset: https://www.kaggle.com/datasets/bhanupratapbiswas/uber-data-analysis

------------------------------------------------------------------------

## Author

Michael Jhin
Built as part of a professional ETL / Data Engineering learning project.