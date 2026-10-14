CREATE OR REPLACE TABLE gsl_databricks.silver.frankfurter_rates
LOCATION 'abfss://silver@adlsgsl.dfs.core.windows.net/frankfurter_rates' AS
SELECT
    CAST(rate_date AS DATE) AS rate_date,
    currency,
    rate,
    base_currency,
    _ingestion_timestamp
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY rate_date, currency
            ORDER BY _ingestion_timestamp DESC
        ) AS rn
    FROM gsl_databricks.bronze.api_data_rates
)
WHERE rn = 1 AND rate > 0