SELECT
    xxhash64(uuid) AS country_key,
    country_name, alpha_2, alpha_3, region, subregion, continents, currencies
FROM gsl_databricks.silver.rest_countries

UNION ALL

SELECT
    -1 AS country_key,
    'Unmapped' AS country_name,
    NULL AS alpha_2,
    NULL AS alpha_3,
    'Unmapped' AS region,
    'Unmapped' AS subregion,
    NULL AS continents,
    NULL AS currencies