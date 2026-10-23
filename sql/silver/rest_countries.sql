CREATE OR REPLACE TABLE gsl_databricks.silver.rest_countries
LOCATION 'abfss://silver@adlsgsl.dfs.core.windows.net/rest_countries'
AS
SELECT
  names.common   AS country_name,
  names.official AS country_official_name,
  NULLIF(codes.alpha_2, '') AS alpha_2,
  NULLIF(codes.alpha_3, '') AS alpha_3,
  region,
  NULLIF(subregion, '') as subregion,
  NULLIF(array_join(continents, ', '), '')AS continents,
  NULLIF(array_join(transform(currencies, c -> c.code), ', '), '') as currencies,
  population,
  classification.un_member AS un_member,
  classification.disputed  AS disputed,
  _ingestion_timestamp
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY uuid
      ORDER BY _ingestion_timestamp DESC
    ) AS rn
  FROM gsl_databricks.bronze.api_data_countries
)
WHERE rn = 1