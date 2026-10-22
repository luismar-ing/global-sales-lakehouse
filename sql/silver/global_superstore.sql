CREATE OR REPLACE TABLE gsl_databricks.silver.global_superstore
LOCATION 'abfss://silver@adlsgsl.dfs.core.windows.net/global_superstore'
AS
SELECT
    Row_ID,
    Order_ID,
    to_date(Order_Date, 'dd-MM-yyyy') AS Order_Date,
    to_date(Ship_Date, 'dd-MM-yyyy') AS Ship_Date,
    Ship_Mode,
    Customer_ID,
    Customer_Name,
    Segment,
    City,
    State,
    Country,
    Market,
    Region,
    CONCAT(Market, ' - ', Region) AS Market_Region,
    Product_ID,
    Category,
    `Sub-Category` AS Sub_Category,
    Product_Name,
    Sales,
    Quantity,
    Discount,
    Profit,
    Shipping_Cost,
    Order_Priority,
    _ingestion_timestamp
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY Row_ID ORDER BY _ingestion_timestamp desc) as rn
    FROM gsl_databricks.bronze.global_superstore
)
WHERE rn = 1
