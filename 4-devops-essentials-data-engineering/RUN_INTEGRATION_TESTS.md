# Running Integration Tests

## Purpose
This notebook verifies that the PySpark transformation functions work together correctly.

## Test Workflow
1. Create a sample customer DataFrame.
2. Filter customers aged 18 or older.
3. Add an age category to each remaining customer.
4. Compare the actual output with the expected DataFrame.

## Expected Result
Three customers should remain:
- Arun — Age 28 — Young
- Kumar — Age 35 — Adult
- Divya — Age 22 — Young

Priya, aged 17, should be excluded.

## Validation
The integration test passes when the actual DataFrame matches the expected DataFrame.
