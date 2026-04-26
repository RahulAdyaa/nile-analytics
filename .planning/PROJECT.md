# Project: Nile Analytics - Ecommerce Pipeline

## Objective
Implement a complete data pipeline for the `ecommerce_sales_34500.csv` dataset, integrating it into the Nile Analytics Django platform. This includes data ingestion, cleaning, transformation, and storage.

## Context
- **Dataset**: `ecommerce_sales_34500.csv` (34,500 records)
- **Current Stack**: Django, SQLite, Pandas (ETL)
- **Target**: Update existing `ETLPipeline` and models to handle new data fields and schema.

## Core Features
- Automatic schema mapping for new CSV headers.
- Data cleaning (handling nulls, invalid dates, duplicates).
- Feature engineering (derived metrics).
- Idempotent database loading.
- Model extension to capture richer customer and sales data.
