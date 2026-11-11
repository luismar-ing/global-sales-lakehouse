WITH anual AS (
  SELECT
    f.source_system,
    c.region,
    d.year,
    SUM(f.amount_usd) AS revenue
  FROM gsl_databricks.gold.fact_sales f
  JOIN gsl_databricks.gold.dim_date d ON f.date_key = d.date_key
  JOIN gsl_databricks.gold.dim_country c ON f.country_key = c.country_key
  GROUP BY f.source_system, c.region, d.year
),
clean_up AS (
    SELECT
        *,
        LAG(revenue) OVER (PARTITION BY region, source_system ORDER BY year ASC) AS revenue_lag
    FROM anual
)
SELECT
    source_system,
    region,
    year,
    revenue,
    try_divide(revenue - revenue_lag, revenue_lag) * 100 AS crecimiento
FROM clean_up
ORDER BY source_system, region, year;