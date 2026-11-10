CREATE OR REPLACE TABLE gsl_databricks.gold.dim_product
LOCATION 'abfss://gold@adlsgsl.dfs.core.windows.net/dim_product'
AS
WITH retail_conteos AS (
  SELECT UPPER(TRIM(StockCode)) AS product_code, Description, COUNT(*) AS n
  FROM gsl_databricks.silver.online_retail
  WHERE transaction_type IN ('sale', 'cancellation')
  GROUP BY UPPER(TRIM(StockCode)), Description
),
retail AS (
  SELECT
    'online_retail' AS source_system,
    product_code AS natural_key,
    product_code,
    Description AS product_name,
    CAST(NULL AS STRING) AS category,
    CAST(NULL AS STRING) AS sub_category
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY product_code ORDER BY n DESC, Description) AS rn
    FROM retail_conteos
  )
  WHERE rn = 1
),
superstore AS (
  SELECT DISTINCT
    'global_superstore' AS source_system,
    CONCAT(Product_ID, '|', Product_Name) AS natural_key,
    Product_ID AS product_code,
    Product_Name AS product_name,
    Category AS category,
    Sub_Category AS sub_category
  FROM gsl_databricks.silver.global_superstore
),
unioned AS (
  SELECT * FROM retail
  UNION ALL
  SELECT * FROM superstore
)
SELECT
  xxhash64(source_system, natural_key) AS product_key,
  source_system,
  natural_key,
  product_code,
  product_name,
  category,
  sub_category
FROM unioned

UNION ALL

SELECT
  -1 AS product_key,
  'Unknown' AS source_system,
  NULL AS natural_key,
  NULL AS product_code,
  'Unknown' AS product_name,
  NULL AS category,
  NULL AS sub_category