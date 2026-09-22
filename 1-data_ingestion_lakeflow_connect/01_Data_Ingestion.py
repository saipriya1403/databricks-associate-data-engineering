# Databricks notebook source
# MAGIC %md
# MAGIC # CTAS Ingestion
# MAGIC
# MAGIC ## Objective
# MAGIC Demonstrate batch ingestion using CREATE TABLE AS (CTAS) in Databricks.
# MAGIC
# MAGIC ## What I learned
# MAGIC - CTAS creates a new table from the result of a query.
# MAGIC - CTAS is suitable for one-time/ad hoc batch ingestion.
# MAGIC - The resulting table is a Delta table by default.
# MAGIC
# MAGIC ## Flow
# MAGIC
# MAGIC Source Data
# MAGIC    ↓
# MAGIC    source_orders
# MAGIC       ↓ CTAS
# MAGIC       bronze_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE source_orders AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Sai', 500.00),
# MAGIC   (2, 'Priya', 750.00),
# MAGIC   (3, 'Anu', 300.00)
# MAGIC   AS t(order_id, customer_name, order_amount);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from source_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table bronze_orders as
# MAGIC select * from source_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: COPY INTO Ingestion
# MAGIC
# MAGIC ### Objective
# MAGIC Demonstrate incremental batch ingestion using COPY INTO in Databricks.
# MAGIC
# MAGIC ### What I learned
# MAGIC - COPY INTO loads data from files into a Delta table.
# MAGIC - It can process new files incrementally.
# MAGIC - Previously loaded files can be skipped when the same source is processed again.
# MAGIC - CSV files can be loaded from a Databricks Volume.
# MAGIC
# MAGIC ### Flow
# MAGIC
# MAGIC CSV File
# MAGIC    ↓
# MAGIC    Databricks Volume
# MAGIC       ↓ COPY INTO
# MAGIC       bronze_orders_copy

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze_orders_copy (
# MAGIC       order_id INT,
# MAGIC         customer_name STRING,
# MAGIC           order_amount DOUBLE
# MAGIC           );
# MAGIC
# MAGIC
# MAGIC   
# MAGIC   
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze_orders_copy
# MAGIC FROM '/Volumes/dataengineering/default/data_ingestion/orders.csv'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC     'inferSchema' = 'true'
# MAGIC  
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_orders_copy

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Auto Loader
# MAGIC
# MAGIC ### Objective
# MAGIC Demonstrate incremental file ingestion using Auto Loader.
# MAGIC
# MAGIC ### What I learned
# MAGIC - Auto Loader processes new files incrementally.
# MAGIC - It is useful when files continuously arrive in a storage location.
# MAGIC - New files can be processed without reprocessing previously loaded files.
# MAGIC
# MAGIC ### Flow
# MAGIC
# MAGIC New CSV Files
# MAGIC    ↓
# MAGIC    Databricks Volume
# MAGIC       ↓ Auto Loader
# MAGIC       Bronze Streaming Table

# COMMAND ----------

source_path =  "/Volumes/dataengineering/default/data_ingestion/"
schema_path = "/Volumes/dataengineering/default/data_ingestion/_schema/"
checkpoint_path = "/Volumes/dataengineering/default/data_ingestion/_checkpoint/"
df = (
    spark.readStream
    .format("CloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", "true")
    .load(source_path)
)













# COMMAND ----------

checkpoint_path = "/Volumes/dataengineering/default/data_ingestion/_checkpoint_autoloader/"
query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("bronze_orders_autoloader")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_orders_autoloader;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_orders_autoloader
# MAGIC order by order_id;

# COMMAND ----------

query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("bronze_orders_autoloader")
                            )


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_orders_autoloader
# MAGIC order by order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Result
# MAGIC
# MAGIC Auto Loader successfully processed CSV files incrementally.
# MAGIC
# MAGIC - Initial files: 7 records
# MAGIC - New file: 2 records
# MAGIC - Final records: 9
# MAGIC - Re-running the same Auto Loader query did not create duplicates.
# MAGIC - Checkpointing tracks previously processed files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Metadata Columns
# MAGIC
# MAGIC ### Objective
# MAGIC Add source file metadata during data ingestion.
# MAGIC
# MAGIC ### What I learned
# MAGIC - Databricks provides the `_metadata` column for input files.
# MAGIC - `_metadata.file_name` identifies the source file.
# MAGIC - `_metadata.file_modification_time` shows when the source file was modified.
# MAGIC - Metadata is useful for auditing, tracing, and debugging data.
# MAGIC
# MAGIC ### Flow
# MAGIC
# MAGIC CSV Files
# MAGIC    ↓
# MAGIC    Databricks Volume
# MAGIC       ↓
# MAGIC       Ingestion + Metadata
# MAGIC          ↓
# MAGIC          Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze_orders_metadata AS
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_name,
# MAGIC     order_amount,
# MAGIC     _metadata.file_name AS source_file_name,
# MAGIC     _metadata.file_modification_time AS source_file_modified_time
# MAGIC FROM read_files(
# MAGIC                 '/Volumes/dataengineering/default/data_ingestion/',
# MAGIC                 format => 'csv',
# MAGIC                 header => true,
# MAGIC                 inferSchema => true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_orders_metadata
# MAGIC order by order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Result
# MAGIC
# MAGIC Metadata columns were successfully added during ingestion.
# MAGIC
# MAGIC - `source_file_name` identifies the source file.
# MAGIC - `source_file_modified_time` captures the file modification timestamp.
# MAGIC - Metadata helps with auditing, lineage, and troubleshooting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Rescued Data Column
# MAGIC
# MAGIC ### Objective
# MAGIC Demonstrate how `_rescued_data` captures values that do not match the expected schema.
# MAGIC
# MAGIC ### What I learned
# MAGIC - `_rescued_data` stores data that cannot be parsed according to the expected schema.
# MAGIC - It helps prevent data loss during ingestion.
# MAGIC - Valid values remain in their normal columns.
# MAGIC - The rescued values are stored as JSON in `_rescued_data`.
# MAGIC
# MAGIC ### Flow
# MAGIC
# MAGIC CSV File
# MAGIC    ↓
# MAGIC    Expected Schema
# MAGIC       ↓
# MAGIC       Schema Mismatch
# MAGIC          ↓
# MAGIC          _rescued_data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze_orders_rescued AS
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC     '/Volumes/dataengineering/default/data_ingestion/orders_rescued.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     schema => 'order_id INT, customer_name STRING, order_amount DECIMAL(10,2)',
# MAGIC     rescuedDataColumn => '_rescued_data'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_orders_rescued;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Result
# MAGIC
# MAGIC The `_rescued_data` column successfully captured a value that did not match the expected schema.
# MAGIC
# MAGIC - Valid records were loaded normally.
# MAGIC - The invalid `order_amount` value was not lost.
# MAGIC - The invalid value was stored as JSON in `_rescued_data`.
# MAGIC - `_file_path` identifies the source file containing the problematic record.
# MAGIC
# MAGIC This helps prevent silent data loss during ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: JSON Ingestion
# MAGIC
# MAGIC ### Objective
# MAGIC Demonstrate how JSON data can be ingested and transformed into structured data.
# MAGIC
# MAGIC ### What I learned
# MAGIC - JSON can contain nested objects and arrays.
# MAGIC - JSON data can initially be stored as a STRING.
# MAGIC - `from_json()` can convert a JSON string into a STRUCT.
# MAGIC - STRUCT allows us to access nested fields directly.
# MAGIC - `schema_of_json()` can be used to derive a schema from a JSON example.
# MAGIC
# MAGIC ### Flow
# MAGIC
# MAGIC JSON File
# MAGIC    ↓
# MAGIC    JSON STRING
# MAGIC       ↓ from_json()
# MAGIC       STRUCT
# MAGIC          ↓
# MAGIC          Extract Nested Fields

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table bronze_orders_json as
# MAGIC select * from read_files(
# MAGIC "/Volumes/dataengineering/default/data_ingestion", format =>'json');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_orders_json

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE bronze_orders_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_name,
# MAGIC     amount,
# MAGIC     address.city AS city,
# MAGIC     address.state AS state
# MAGIC FROM bronze_orders_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze_orders_json AS
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC     '/Volumes/dataengineering/default/data_ingestion/orders.json',
# MAGIC         format => 'json'
# MAGIC         );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     customer_name,
# MAGIC     amount,
# MAGIC     address.city AS city,
# MAGIC     address.state AS state
# MAGIC FROM bronze_orders_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE json_string_demo AS
# MAGIC SELECT * FROM VALUES
# MAGIC (
# MAGIC   '{"order_id":301,"customer_name":"Priya","amount":850.00}'
# MAGIC   ),
# MAGIC   (
# MAGIC     '{"order_id":302,"customer_name":"Anu","amount":550.00}'
# MAGIC     )
# MAGIC AS t(json_data);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json_string_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     from_json(
# MAGIC             json_data,
# MAGIC                     'order_id INT, customer_name STRING, amount DOUBLE'
# MAGIC                         ) AS order_struct
# MAGIC FROM json_string_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_struct.order_id AS order_id,
# MAGIC     order_struct.customer_name AS customer_name,
# MAGIC     order_struct.amount AS amount
# MAGIC FROM (
# MAGIC         SELECT
# MAGIC         from_json(
# MAGIC                 json_data,
# MAGIC                 'order_id INT, customer_name STRING, amount DOUBLE'
# MAGIC                 ) AS order_struct
# MAGIC         FROM json_string_demo
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schema_of_json(
# MAGIC       '{"order_id":301,"customer_name":"Priya","amount":850.00}'
# MAGIC       ) AS json_schema;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: MERGE INTO
# MAGIC
# MAGIC ### Objective
# MAGIC Demonstrate how MERGE INTO can update existing records and insert new records into a Delta table.
# MAGIC
# MAGIC ### What I learned
# MAGIC - MERGE INTO can update existing records.
# MAGIC - MERGE INTO can insert new records.
# MAGIC - It is useful for incremental data loads and synchronization.
# MAGIC - MERGE operations are atomic on Delta tables.
# MAGIC
# MAGIC ### Flow
# MAGIC
# MAGIC Target Delta Table
# MAGIC         +
# MAGIC         Incoming Data
# MAGIC                 ↓
# MAGIC                     MERGE INTO
# MAGIC                             ↓
# MAGIC                             Updated Target Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE merge_target AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (1, 'Arun', 500),
# MAGIC     (2, 'Meena', 800),
# MAGIC     (3, 'Kiran', 300)
# MAGIC AS t(customer_id, customer_name, amount);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM merge_target
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW merge_source AS
# MAGIC SELECT * FROM VALUES
# MAGIC     (2, 'Meena', 1000),
# MAGIC     (4, 'Priya', 700)
# MAGIC AS t(customer_id, customer_name, amount);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO merge_target AS target
# MAGIC USING merge_source AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.customer_name = source.customer_name,
# MAGIC     target.amount = source.amount
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (customer_id, customer_name, amount)
# MAGIC     VALUES (source.customer_id, source.customer_name, source.amount);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM merge_target
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY merge_target;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM merge_target VERSION AS OF 0
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM merge_target
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
