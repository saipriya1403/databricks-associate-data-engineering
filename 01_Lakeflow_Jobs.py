# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Dataset

# COMMAND ----------

data = [
        (1, "Arun", 500),
        (2, "Meena", 800),
        (3, "Kiran", 300),
        (4, "Priya", 700),
        (5, "Divya", 900)
                        ]

columns = ["customer_id", "customer_name", "amount"]

df = spark.createDataFrame(data, columns)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Data

# COMMAND ----------



prepared_df = df.filter(df.amount > 0)

display(prepared_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Validation

# COMMAND ----------

invalid_count = prepared_df.filter(prepared_df.amount <= 0).count()

if invalid_count == 0:
    print("Validation passed: all records are valid.")
else:
    print(f"Validation failed: {invalid_count} invalid records found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Transformation

# COMMAND ----------

final_df = (
        prepared_df
            .select("customer_id", "customer_name", "amount")
                .orderBy("customer_id")
                )

display(final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Create a simple quality result

# COMMAND ----------

record_count = final_df.count()

if record_count == 5:
    print(f"Quality check passed: {record_count} records found.")
else:
    print(f"Quality check failed: {record_count} records found.")

# COMMAND ----------

# Save final result for the Lakeflow Job
final_df.write.mode("overwrite").saveAsTable("module2_final_customers")

print("Final customer table created successfully.")

# COMMAND ----------

# Save final result for the Lakeflow Job
final_df.write.mode("overwrite").saveAsTable("module2_final_customers")

print("Final customer table created successfully.")