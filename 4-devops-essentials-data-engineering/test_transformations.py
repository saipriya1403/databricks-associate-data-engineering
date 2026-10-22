
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual
from transformations import filter_adult_customers, add_age_category

spark = SparkSession.builder.getOrCreate()

def test_filter_adult_customers():
    data = [(1, "Arun", 28), (2, "Priya", 17)]
    columns = ["CustomerId", "CustomerName", "Age"]
    df = spark.createDataFrame(data, columns)

    result = filter_adult_customers(df)

    expected = spark.createDataFrame(
        [(1, "Arun", 28)],
        columns
    )

    assertDataFrameEqual(result, expected)

def test_add_age_category():
    data = [(1, "Arun", 28), (2, "Kumar", 35)]
    columns = ["CustomerId", "CustomerName", "Age"]
    df = spark.createDataFrame(data, columns)

    result = add_age_category(df)

    expected = spark.createDataFrame(
        [(1, "Arun", 28, "Young"), (2, "Kumar", 35, "Adult")],
        ["CustomerId", "CustomerName", "Age", "AgeCategory"]
    )

    assertDataFrameEqual(result, expected)

