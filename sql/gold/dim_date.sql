CREATE SCHEMA IF NOT EXISTS gsl_databricks.gold;

CREATE OR REPLACE TABLE gsl_databricks.gold.dim_date
LOCATION 'abfss://gold@adlsgsl.dfs.core.windows.net/dim_date'
AS
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
    d AS full_date,
    year(d) AS year,
    quarter(d) AS quarter,
    month(d) AS month,
    date_format(d, 'MMMM') AS month_name,
    dayofmonth(d) AS day_of_month,
    dayofweek(d) AS day_of_week,
    dayofweek(d) IN (1, 7) AS is_weekend
FROM (
    SELECT explode(sequence(DATE'2009-12-01', DATE'2014-12-31', INTERVAL 1 DAY)) AS d
)