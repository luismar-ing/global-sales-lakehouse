WITH monthly AS (
  SELECT
    f.source_system,
    c.region,
    d.year,
    d.month,
    SUM(f.amount_usd) AS revenue
  FROM gsl_databricks.gold.fact_sales f
  JOIN gsl_databricks.gold.dim_date d ON f.date_key = d.date_key
  JOIN gsl_databricks.gold.dim_country c ON f.country_key = c.country_key
  -- Diciembre de 2011 solo tiene 9 días de Online Retail
  WHERE NOT (f.source_system = 'online_retail' AND d.year = 2011 AND d.month = 12)
  GROUP BY f.source_system, c.region, d.year, d.month
),
with_lag AS (
  SELECT
    *,
    LAG(revenue) OVER (PARTITION BY source_system, region, month ORDER BY year) AS revenue_prev_year,
    LAG(year)    OVER (PARTITION BY source_system, region, month ORDER BY year) AS year_prev
  FROM monthly
)
SELECT
  source_system,
  region,
  year,
  month,
  revenue,
  revenue_prev_year,
  CASE
    WHEN year_prev = year - 1
      THEN try_divide(revenue - revenue_prev_year, revenue_prev_year) * 100
  END AS crecimiento_anual_pct
FROM with_lag
ORDER BY source_system, region, month, year;