from pyspark.sql import functions as F
from pyspark.sql.types import StructType

LANDING_PATH = "abfss://landing@adlsgsl.dfs.core.windows.net/"
BRONZE_PATH = "abfss://bronze@adlsgsl.dfs.core.windows.net/"

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
def add_audit_columns(df, filename: str):
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
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

