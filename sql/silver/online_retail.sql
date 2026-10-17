CREATE OR REPLACE TABLE gsl_databricks.silver.online_retail
LOCATION 'abfss://silver@adlsgsl.dfs.core.windows.net/online_retail'
AS
SELECT
    Invoice,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    Price,
    CASE
        WHEN transaction_type = 'sale' AND Customer_ID IS NULL THEN 'GUEST'
        ELSE Customer_ID
    END AS Customer_ID,
    Country,
    transaction_type,
    _ingestion_timestamp
FROM (
    SELECT
        *,
        CASE
            WHEN UPPER(TRIM(Description)) = 'UPDATE' then 'invalid'
            WHEN UPPER(TRIM(StockCode)) LIKE 'TEST%' then 'invalid'
            WHEN UPPER(TRIM(Description)) = 'EBAY' AND Quantity < 0 AND Invoice NOT LIKE 'C%' THEN 'invalid'
            WHEN UPPER(TRIM(StockCode)) LIKE 'ADJUST%' THEN 'manual_adjustment'
            WHEN UPPER(TRIM(StockCode)) LIKE 'GIFT%' THEN 'gift_voucher'
            WHEN UPPER(TRIM(StockCode)) IN ('POST', 'DOT', 'C2', 'M', 'D', 'S', 'BANK CHARGES', 'AMAZONFEE', 'CRUK', 'B')
                AND Invoice LIKE 'C%' AND Quantity < 0 THEN 'fee_reversal'
            WHEN UPPER(TRIM(StockCode)) IN ('POST', 'DOT', 'C2', 'M', 'D', 'S', 'BANK CHARGES', 'AMAZONFEE', 'CRUK', 'B')
                THEN 'fee_or_charge'
            WHEN Quantity < 0 AND Invoice NOT LIKE 'C%' THEN 'internal_adjustment'
            WHEN Invoice LIKE 'C%' AND (StockCode RLIKE '^[0-9]{4,}[A-Za-z]*$' OR StockCode IN ('DCGSSBOY','DCGSSGIRL','PADS','SP1002','47503J')) THEN 'cancellation'
            WHEN Quantity > 0 AND (StockCode RLIKE '^[0-9]{4,}[A-Za-z]*$' OR StockCode IN ('DCGSSBOY','DCGSSGIRL','PADS','SP1002','47503J')) AND Description IS NOT NULL THEN 'sale'
            ELSE 'unclassified'
        END AS transaction_type,
        ROW_NUMBER() OVER (PARTITION BY Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer_ID ORDER BY _ingestion_timestamp DESC) AS rn
    FROM gsl_databricks.bronze.online_retail
)
WHERE rn = 1 AND transaction_type != 'invalid'

