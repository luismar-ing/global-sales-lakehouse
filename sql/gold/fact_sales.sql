CREATE OR REPLACE TABLE gsl_databricks.gold.fact_sales
LOCATION 'abfss://gold@adlsgsl.dfs.core.windows.net/fact_sales'
AS
WITH retail_lines AS (
  SELECT
    'online_retail' AS source_system,
    Invoice AS transaction_id,
    transaction_type,
    CAST(InvoiceDate AS DATE) AS sale_date,
    regexp_replace(Customer_ID, '\\.0$', '') AS customer_id,
    UPPER(TRIM(StockCode)) AS product_natural_key,
    Country AS country_source,
    Quantity AS quantity,
    Price AS unit_price_local,
    Quantity * Price AS amount_local,
    'GBP' AS currency,
    CAST(NULL AS DOUBLE) AS discount,
    CAST(NULL AS DOUBLE) AS profit_local,
    CAST(NULL AS DOUBLE) AS shipping_cost_local,
    Price = 0 AS is_zero_price
  FROM gsl_databricks.silver.online_retail
  WHERE transaction_type IN ('sale', 'cancellation')
),
superstore_lines AS (
  SELECT
    'global_superstore' AS source_system,
    Order_ID AS transaction_id,
    'sale' AS transaction_type,
    Order_Date AS sale_date,
    Customer_ID AS customer_id,
    CONCAT(Product_ID, '|', Product_Name) AS product_natural_key,
    Country AS country_source,
    Quantity AS quantity,
    Sales / NULLIF(Quantity, 0) AS unit_price_local,
    Sales AS amount_local,
    'USD' AS currency,
    Discount AS discount,
    Profit AS profit_local,
    Shipping_Cost AS shipping_cost_local,
    Sales = 0 AS is_zero_price
  FROM gsl_databricks.silver.global_superstore
),
lines AS (
  SELECT * FROM retail_lines
  UNION ALL
  SELECT * FROM superstore_lines
)
SELECT
  CAST(date_format(l.sale_date, 'yyyyMMdd') AS INT) AS date_key,
  COALESCE(c.country_key, -1) AS country_key,
  COALESCE(cu.customer_key, -1) AS customer_key,
  COALESCE(p.product_key, -1) AS product_key,
  l.source_system,
  l.transaction_id,
  l.transaction_type,
  l.quantity,
  l.unit_price_local,
  l.amount_local,
  l.currency,
  CASE
    WHEN l.currency = 'USD' THEN 1.0
    ELSE r.rate_filled
  END AS fx_rate,
  CASE
    WHEN l.currency = 'USD' THEN 'N/A'
    ELSE r.fill_type
  END AS fx_fill_type,
  CASE
    WHEN l.currency = 'GBP' THEN l.amount_local / r.rate_filled
    ELSE l.amount_local
  END AS amount_usd,
  CASE
    WHEN l.currency = 'GBP' THEN l.profit_local / r.rate_filled
    ELSE l.profit_local
  END AS profit_usd,
  CASE
    WHEN l.currency = 'GBP' THEN l.shipping_cost_local / r.rate_filled
    ELSE l.shipping_cost_local
  END AS shipping_cost_usd,
  l.discount,
  l.is_zero_price
FROM lines l
LEFT JOIN gsl_databricks.silver.countries_alias a ON l.country_source = a.source_name
LEFT JOIN gsl_databricks.gold.dim_country c
       ON COALESCE(a.country_name, l.country_source) = c.country_name
LEFT JOIN gsl_databricks.gold.dim_customer cu
       ON l.source_system = cu.source_system AND l.customer_id = cu.customer_id
LEFT JOIN gsl_databricks.gold.dim_product p
       ON l.source_system = p.source_system AND l.product_natural_key = p.natural_key
LEFT JOIN gsl_databricks.gold.daily_rates r
       ON l.sale_date = r.full_date AND l.currency = r.currency