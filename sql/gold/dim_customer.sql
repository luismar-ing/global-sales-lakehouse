CREATE OR REPLACE TABLE gsl_databricks.gold.dim_customer
LOCATION 'abfss://gold@adlsgsl.dfs.core.windows.net/dim_customer'
AS
WITH retail AS (
  SELECT DISTINCT
    'online_retail' AS source_system,
    regexp_replace(Customer_ID, '\\.0$', '') AS customer_id,
    CAST(NULL AS STRING) AS customer_name,
    CAST(NULL AS STRING) AS segment
  FROM gsl_databricks.silver.online_retail
  WHERE transaction_type IN ('sale', 'cancellation')
    AND Customer_ID IS NOT NULL
),
superstore AS (
  SELECT DISTINCT
    'global_superstore' AS source_system,
    Customer_ID AS customer_id,
    Customer_Name AS customer_name,
    Segment AS segment
  FROM gsl_databricks.silver.global_superstore
),
unioned AS (
  SELECT * FROM retail
  UNION ALL
  SELECT * FROM superstore
)
SELECT
  xxhash64(source_system, customer_id) AS customer_key,
  source_system,
  customer_id,
  customer_name,
  segment,
  customer_id = 'GUEST' AS is_guest
FROM unioned

UNION ALL

SELECT
  -1 AS customer_key,
  'Unknown' AS source_system,
  NULL AS customer_id,
  'Unknown' AS customer_name,
  NULL AS segment,
  false AS is_guest