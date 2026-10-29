CREATE OR REPLACE TABLE gsl_databricks.silver.countries_alias
LOCATION 'abfss://silver@adlsgsl.dfs.core.windows.net/countries_alias'
AS
SELECT * FROM VALUES
  ('EIRE', 'Ireland'),
  ('USA', 'United States'),
  ('RSA', 'South Africa'),
  ('Czech Republic', 'Czechia'),
  ('Myanmar (Burma)', 'Myanmar'),
  ('Cote d\'Ivoire', 'Ivory Coast'),
  ('Macedonia', 'North Macedonia'),
  ('Swaziland', 'Eswatini'),
  ('Republic of the Congo', 'Congo'),
  ('Democratic Republic of the Congo', 'DRC'),
  ('Korea', 'South Korea')
AS t(source_name, country_name);