WITH top_products AS (
    SELECT
        f.source_system,
        f.product_key,
        dp.product_name,
        SUM(f.quantity) as total_qty
    FROM gsl_databricks.gold.fact_sales f
    JOIN gsl_databricks.gold.dim_product dp
        ON dp.product_key = f.product_key
    WHERE f.transaction_type = 'sale'
        AND is_zero_price IS FALSE
        AND f.product_key != -1
    GROUP BY f.source_system, f.product_key, dp.product_name
)
SELECT
    *
FROM top_products
ORDER BY total_qty DESC
LIMIT 5;


WITH top_revenue AS (
    SELECT
        f.source_system,
        f.product_key,
        dp.product_name,
        SUM(f.amount_usd) AS total_revenue
    FROM gsl_databricks.gold.fact_sales f
    JOIN gsl_databricks.gold.dim_product dp
        ON dp.product_key = f.product_key
    WHERE f.transaction_type = 'sale'
        AND f.product_key != -1
    GROUP BY f.source_system, f.product_key, dp.product_name
)
SELECT
    *
FROM top_revenue
ORDER BY total_revenue DESC
LIMIT 5;
