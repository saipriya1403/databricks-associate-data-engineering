# Module 4: DevOps Essentials for Data Engineering

## Overview
This module demonstrates DevOps practices for data engineering using Databricks and PySpark.

## Topics Covered
- Modularizing PySpark transformations
- Unit testing with Pytest
- Integration testing
- Data validation
- Databricks Jobs and task dependencies

## Project Files
- `transformations.py` – reusable PySpark transformation functions
- `test_transformations.py` – unit tests for transformation functions
- `Run_Units_Tests` – notebook to execute unit tests
- `Run_Integration_Tests` – notebook to test the integrated transformations
- `Final_Validation` – notebook to validate the final output

## Job Workflow
Unit Tests → Integration Tests → Final Validation
## Databricks Job Execution

**Job Name:** `Module4_DevOps_Testing_Job`

The job contains three dependent tasks:

1. **Unit_Tests** — Executes PySpark unit tests using Pytest.
2. **Integration_Tests** — Validates that the transformation functions work together correctly.
3. **Final_Validation** — Checks the final customer count against the expected count.

**Execution Status:** Successful

All three tasks completed successfully in Databricks.


## Validation Results
All three tasks completed successfully in the Databricks Job.

## Tools Used
- Databricks
- PySpark
- Python
- Pytest
- GitHub
