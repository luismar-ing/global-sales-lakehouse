SELECT
    d.year,
    d.month,
    SUM(f.amount_usd) AS revenue_bruto
FROM gsl_databricks.gold.fact_sales f
JOIN gsl_databricks.gold.dim_date d
    ON f.date_key = d.date_key
WHERE f.transaction_type = 'sale' --ganancia bruta
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

SELECT
    d.year,
    d.month,
    d.day_of_month,
    COALESCE(SUM(f.amount_usd), 0) AS revenue_bruto
FROM gsl_databricks.gold.dim_date d
LEFT JOIN gsl_databricks.gold.fact_sales f
    ON f.date_key = d.date_key
    AND f.transaction_type = 'sale' -- ganancia bruta
GROUP BY d.year, d.month, d.day_of_month
ORDER BY d.year, d.month, d.day_of_month;