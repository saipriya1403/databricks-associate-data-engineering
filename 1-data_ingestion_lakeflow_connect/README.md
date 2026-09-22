# Data Ingestion with Lakeflow Connect

## Overview

Hands-on implementation of data ingestion concepts learned through the
Databricks Associate Data Engineering pathway.

## Concepts Implemented

- CTAS batch ingestion
- COPY INTO incremental ingestion
- Auto Loader
- Metadata columns
- Rescued data column
- JSON ingestion
- JSON STRING to STRUCT
- MERGE INTO
- Delta Lake Time Travel

## Environment

- Databricks Community Edition
- SQL
- Python
- Delta Lake
- Databricks Volumes

## Implementation

### 1. CTAS
Batch ingestion using CREATE TABLE AS.

### 2. COPY INTO
Incremental file ingestion from CSV files.

### 3. Auto Loader
Incremental file processing using Auto Loader and checkpointing.

### 4. Metadata Columns
Captured source file name and modification time.

### 5. Rescued Data
Captured schema-mismatched data using `_rescued_data`.

### 6. JSON Ingestion
Read JSON data, worked with nested fields, and converted JSON strings into STRUCT using `from_json()`.

### 7. MERGE INTO
Updated existing records and inserted new records using MERGE INTO.

### 8. Delta Time Travel
Queried an earlier table version using `VERSION AS OF`.

## Key Learning

This project demonstrates practical data ingestion patterns for building
Bronze-layer pipelines in Databricks.
