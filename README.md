
# Financial ETL Engine

An end-to-end financial transaction data pipeline built with Python,
Pandas, Parquet, DuckDB, SQLite, pytest, and Windows scheduling.

## Overview

This project demonstrates how to ingest transaction data in batches,
validate data quality, build analytical models, and orchestrate pipeline
execution with logging, monitoring, and failure reporting.

## Key Features

- Chunk-based CSV ingestion for memory-efficient processing
- Parquet-based intermediate data storage
- Transaction validation and rejected-record quarantine
- Schema baseline and schema-drift checks
- Incremental ingestion using SHA-256 file fingerprints
- SQLite batch registry for ingestion idempotency
- DuckDB analytical models and SQL summaries
- Multi-batch loading with duplicate-ID checks and transactions
- Automated tests using pytest
- Pipeline orchestration with fail-fast behavior
- JSON run reports, logging, and basic failure-recovery support
- Windows Task Scheduler integration

## Technology Stack

- Python
- Pandas
- PyArrow / Parquet
- DuckDB
- SQLite
- SQL
- pytest
- Windows CMD and Task Scheduler
- Git and GitHub

## Project Structure

```text
financial-etl-engine/
├── src/
│   ├── 01_ingest.py
│   ├── 02_quality.py
│   ├── 04_clean_data.py
│   ├── 06_build_models.py
│   ├── 07_query_models.py
│   ├── 08_incremental_ingest.py
│   ├── 09_schema_check.py
│   ├── 10_validate_incremental.py
│   ├── 11_build_incremental_models.py
│   ├── 12_query_incremental_models.py
│   ├── 13_load_batches.py
│   ├── 14_check_batch_loads.py
│   ├── 15_monitor_pipeline.py
│   ├── 16_run_pipeline.py
│   ├── 17_run_history.py
│   └── 18_recovery_runner.py
├── sql/
├── tests/
├── data/                 # Local data; not committed
├── .gitignore
└── README.md
```

The structure above summarizes the main modules. Some supporting
files may differ as the project evolves.

## Dataset

The project uses the IEEE-CIS Fraud Detection dataset from Kaggle:

https://www.kaggle.com/competitions/ieee-fraud-detection/data

Download the competition data according to Kaggle's access requirements
and place `train_transaction.csv` in:

`data/raw/train_transaction.csv`

The dataset is not included in this repository. Users must obtain it
from Kaggle and comply with its terms.

## Local Setup

Create and activate a virtual environment on Windows:

```bat
python -m venv .venv
.venv\Scripts\activate
```

Install the dependencies used by the project:

```bat
pip install pandas pyarrow duckdb pytest
```

Install any additional packages required by the modules you run.

## Run Tests

```bat
python -m pytest tests\test_quality_rules.py -v
```

## Run the Pipeline

Run the orchestrator from the project root:

```bat
python src\16_run_pipeline.py
```

Or run the recovery runner:

```bat
python src\18_recovery_runner.py
```

For Windows Task Scheduler, use:

`run_scheduled_pipeline.bat`

## Outputs

The pipeline creates local data, database tables, logs, validation
reports, run-history reports, and failure incident reports under `data/`.
These are generated locally and are intentionally excluded from Git.

## Current Scope and Limitations

- Designed as a local portfolio project, not a production deployment.
- Incremental processing is based on local files and a SQLite registry.
- DuckDB provides local analytical storage.
- Windows Task Scheduler provides basic scheduling.
- External alert delivery, cloud deployment, distributed processing,
  and production-grade access controls are not implemented.

## Future Improvements

- CI checks with GitHub Actions
- Dependency pinning in `requirements.txt`
- Data-quality and pipeline integration tests
- Cloud object storage and managed orchestration
- Structured metrics and external alerting

## Disclaimer

This is an educational portfolio project. Model and pipeline outputs
should not be interpreted as proof of financial savings or as a
production fraud-prevention service.

