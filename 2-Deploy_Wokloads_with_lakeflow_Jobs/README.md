# Deploy Workloads with Lakeflow Jobs

## Objective

Demonstrate how Databricks Lakeflow Jobs can orchestrate a simple data engineering workflow using dependent notebook tasks.

## Project Overview

This project demonstrates a basic workflow where multiple tasks are executed in a defined order.

### Workflow

```text
Create Dataset
      ↓
Validate Data
      ↓
Final Transformation
      ↓
Quality Check
```

## Tasks Implemented

### Task 1: Create Dataset

Created a sample customer dataset using PySpark.

The dataset contains:

* Customer ID
* Customer Name
* Amount

### Task 2: Validate Data

Validated the prepared customer data by checking that the amount is positive.

### Task 3: Final Transformation

Selected the required customer columns and created the final transformed dataset.

### Task 4: Quality Check

Checked the final record count to verify that the expected number of records was produced.

The final result was saved as the Delta table:

```text
module2_final_customers
```

## Lakeflow Job Configuration

A Lakeflow Job was created with four dependent notebook tasks:

```text
create_dataset
      ↓
validate_data
      ↓
final_transformation
      ↓
quality_check
```

Each task depends on the successful completion of the previous task.

## Result

The Lakeflow Job was executed successfully, and all four tasks completed successfully.

## Key Concepts Learned

* Lakeflow Jobs
* Jobs and Tasks
* Task dependencies
* Workflow orchestration
* Notebook tasks
* Sequential task execution
* Data validation
* Data quality checks
* Delta table creation

## Technologies Used

* Databricks
* PySpark
* Python
* Delta Lake
* Lakeflow Jobs

## Project Structure

```text
02-deploy-workloads-lakeflow-jobs/
├── 01_Lakeflow_Jobs.py
└── README.md
```
