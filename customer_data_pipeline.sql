--Create the bronze streaming table
CREATE OR REFRESH STREAMING TABLE customers_bronze
AS
SELECT *
FROM STREAM read_files(
    '${input_path}',
        format => 'json'
        );

-- Create the silver streaming table
--CREATE OR REFRESH STREAMING TABLE customers_silver
--AS
--SELECT
    --CustomerId,
    --CustomerName,
    --City,
    --Age
--FROM STREAM customers_bronze
--WHERE Age >= 18;
-- Add a Data Quality Expectation
CREATE OR REFRESH STREAMING TABLE customers_silver
(
    CONSTRAINT valid_customer_age EXPECT (Age >= 18)
    )
    AS
    SELECT
    CustomerId,
    CustomerName,
    City,
    Age
FROM STREAM customers_bronze;
-- Create the Gold table
CREATE OR REFRESH MATERIALIZED VIEW customers_gold
AS
SELECT
    City,
        COUNT(*) AS CustomerCount,
        AVG(Age) AS AverageAge
        FROM customers_silver
        GROUP BY City;



    





   













