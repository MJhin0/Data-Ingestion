Data Ingestion Pipeline

A modular and test-driven ETL (Extract, Transform, Load) pipeline built in Python.
The pipeline ingests CSV files, cleans and validates the data, then loads it into a PostgreSQL database.
Unit tests are included using pytest.

Project Structure
project/
│── ingestion/
│   └── src/
│       ├── readers/
│       │   └── csv_reader.py
│       ├── clean.py
│       ├── validate.py
│       ├── load.py
│       └── logger.py
│
│── config/
│   └── sources.yml
│
│── data/
│   ├── UberDataset.csv
│   ├── UberDataset_validated.csv
│   └── UberDataset_rejected.csv
│
│── test/
│   ├── test_clean.py
│   ├── test_csv_reader.py
│   ├── test_validate.py
│   └── test_load.py
│
│── main.py
│── pytest.ini
│── requirements.txt

Features

Extract
Loads CSV files into pandas DataFrames.

Transform
    Cleans:
        Missing data
        Duplicates
        Normalizes column names
        Standardizes string formatting

    Validates:
        Schema (data types)
        Custom business rules
        Produces _validated.csv and _rejected.csv

Load
    Loads validated data into PostgreSQL
    Automatically adds a UUID primary key (id)
    Uses SQLAlchemy for engine management

Testing
    Full unit test suite using pytest
    All tests pass (10 passed)


Source of dataset: https://www.kaggle.com/datasets/bhanupratapbiswas/uber-data-analysis