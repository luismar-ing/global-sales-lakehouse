SELECT
    source_system,
    SUM(amount_usd) / COUNT(DISTINCT transaction_id) AS avg_ticket
FROM gsl_databricks.gold.fact_sales
WHERE transaction_type = 'sale'
GROUP BY source_system;