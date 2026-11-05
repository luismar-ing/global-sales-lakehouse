CREATE OR REPLACE TABLE gsl_databricks.gold.daily_rates
LOCATION 'abfss://gold@adlsgsl.dfs.core.windows.net/daily_rates'
AS
WITH grid AS (
    SELECT d.full_date, c.currency
    FROM gsl_databricks.gold.dim_date d
    CROSS JOIN (SELECT DISTINCT currency FROM gsl_databricks.silver.frankfurter_rates) c
),
joined AS (
    SELECT g.full_date, g.currency, r.rate
    FROM grid g
    LEFT JOIN gsl_databricks.silver.frankfurter_rates r
        ON g.full_date = r.rate_date AND g.currency = r.currency
),
filled AS (
    SELECT
        full_date,
        currency,
        rate,
        -- ultimo rate no nulo hacia atras (forward fill)
        last_value(rate, true) OVER (
            PARTITION BY currency ORDER BY full_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prev_rate,
        -- primer rate no nulo hacia adelante (backward fill)
        first_value(rate, true) OVER (
            PARTITION BY currency ORDER BY full_date
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) as next_rate
    FROM joined
)
SELECT
    full_date,
    currency,
    -- opta primero por un forward fill,
    -- en caso de ser nulo en vez de dejar la fecha sin rate, se aplica un backward fill
    COALESCE(prev_rate, next_rate) AS rate_filled,
    CASE
        WHEN rate IS NOT NULL THEN 'actual'
        WHEN prev_rate IS NOT NULL THEN 'forward'
        WHEN next_rate IS NOT NULL THEN 'backward'
        ELSE 'missing'
    END AS fill_type
FROM filled
