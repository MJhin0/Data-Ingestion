# Data Ingestion Pipeline

A modular, test-driven **ETL (Extract → Transform → Load)** pipeline in Python.  
This project reads CSV files, cleans and validates the data, and loads validated rows into a PostgreSQL database. Unit tests are included (pytest).

---

## Project Structure

project/
├── ingestion/
│ └── src/
│ ├── readers/
│ │ └── csv_reader.py
│ ├── clean.py
│ ├── validate.py
│ ├── load.py
│ └── logger.py
│
├── config/
│ └── sources.yml
│
├── data/
│ ├── UberDataset.csv
│ ├── UberDataset_validated.csv
│ └── UberDataset_rejected.csv
│
├── test/
│ ├── test_clean.py
│ ├── test_csv_reader.py
│ ├── test_validate.py
│ └── test_load.py
│
├── main.py
├── pytest.ini
└── requirements.txt

---

## Features

- **Extract:** Read CSV files into `pandas` DataFrames.  
- **Transform / Clean:** Normalize columns, trim strings, drop missing rows and full-row duplicates.  
- **Validate:** Enforce schema (types) and business rules; produce reject reasons.  
- **Load:** Write validated data to PostgreSQL using SQLAlchemy; add UUID primary key.  
- **Testing:** Unit tests implemented with `pytest`. Mocked DB tests included.

---

Source of dataset: https://www.kaggle.com/datasets/bhanupratapbiswas/uber-data-analysis