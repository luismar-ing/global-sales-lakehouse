from typing import Any
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType)

LANDING_PATH = "abfss://landing@adlsgsl.dfs.core.windows.net/"
BRONZE_PATH = "abfss://bronze@adlsgsl.dfs.core.windows.net/"

def get_source_hash(spark, filename: str) -> str:
    path = LANDING_PATH + filename + ".sha256"

    hash_str = spark.read.text(path).first()[0]
    return hash_str

def is_already_ingested(spark, filename: str, table_name: str) -> tuple[bool, str]:
    full_table_name = "gsl_databricks.bronze." + table_name
    source_hash = get_source_hash(spark, filename)

    if not spark.catalog.tableExists(full_table_name):
        return False, source_hash

    count = spark.sql(f"""
        SELECT COUNT(*) FROM {full_table_name}
        WHERE _source_file = '{filename}'
        AND _source_hash = '{source_hash}'
    """).first()[0]

    return count > 0, source_hash

# Lee un CSV crudo desde landing.
def read_raw_csv(spark, filename: str, schema: StructType = None):
    path = LANDING_PATH + filename

    reader = (
        spark.read.format("csv").option("header", True)
    )

    if schema is not None:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", True)

    return reader.load(path)

# Agrega metadata de auditoría antes de escribir a Bronze
def add_audit_columns(df, filename: str, hash_str: str):
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
        .withColumn("_source_hash", F.lit(hash_str))
    )

# Escribe el DataFrame como una external Delta table dentro de gsl_databricks.bronze.
def write_bronze_table(df, table_name: str, location: str):
    table_path = location.rstrip("/") + "/" + table_name

    (
        df.write
        .format("delta")
        .mode("append")
        .option("path", table_path)
        .saveAsTable(f"gsl_databricks.bronze.{table_name}")
    )

def flatten_frankfurter_rates(raw_json_str: str) -> list[tuple]:
    parsed = json.loads(raw_json_str)

    base_currency = parsed["base"]
    rows = []

    for date_str, currency_dict in parsed["rates"].items():
        for currency_code, rate in currency_dict.items():
            rows.append(
                (
                    date_str,
                    currency_code,
                    float(rate),
                    base_currency
                )
            )
    return rows

def read_frankfurter_bronze(spark, filename: str):
    path = LANDING_PATH + filename

    # Lee el archivo completo como una sola cadena
    raw_text = spark.sparkContext.wholeTextFiles(path).first()[1]

    rows = flatten_frankfurter_rates(raw_text)

    schema = StructType([
        StructField("rate_date", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("rate", DoubleType(), False),
        StructField("base_currency", StringType(), False)
    ])

    return spark.createDataFrame(rows, schema)

def read_rest_countries_bronze(spark, filename: str):
    path = LANDING_PATH + filename

    return (
        spark.read
        .option("multiline", True)
        .json(path)
    )