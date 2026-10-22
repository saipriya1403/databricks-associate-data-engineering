
from pyspark.sql.functions import col, when

def filter_adult_customers(df):
    return df.filter(col("Age") >= 18)

def add_age_category(df):
    return df.withColumn(
        "AgeCategory",
        when(col("Age") < 30, "Young")
        .otherwise("Adult")
    )
