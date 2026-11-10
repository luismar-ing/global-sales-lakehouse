from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

LANDING_FILE = "abfss://landing@adlsgsl.dfs.core.windows.net/Global_Superstore2.csv"

# Mismo schema que usas en Bronze (fechas como texto)
schema = StructType([
    StructField("Row_ID", IntegerType(), True),
    StructField("Order_ID", StringType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Ship_Date", StringType(), True),
    StructField("Ship_Mode", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Customer_Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Postal_Code", StringType(), True),
    StructField("Market", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Product_ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product_Name", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True),
    StructField("Shipping_Cost", DoubleType(), True),
    StructField("Order_Priority", StringType(), True),
])

# Las 300 filas con Sales nulo en Bronze
null_ids = spark.table("gsl_databricks.bronze.global_superstore") \
                .where("Sales IS NULL") \
                .select("Row_ID")

print("Filas con Sales nulo:", null_ids.count())

raw = spark.read.option("header", True).csv(LANDING_FILE)

# Al leer sin schema, los nombres salen de la cabecera del CSV (con espacios)
raw_ids = raw.withColumn("Row_ID", F.col("Row ID").cast("int"))

raw_ids.join(null_ids, "Row_ID") \
       .select("Row_ID", "Product Name", "Sales", "Quantity", "Discount", "Profit") \
       .show(10, truncate=False)

test = (spark.read
        .option("header", True)
        .option("escape", '"')
        .schema(schema)
        .csv(LANDING_FILE))

test.select(F.count("*").alias("total"),
            F.count("Sales").alias("sales"),
            F.count("Quantity").alias("qty"),
            F.count("Discount").alias("disc"),
            F.count("Profit").alias("profit"),
            F.count("Shipping_Cost").alias("ship")).show()

bronze_rows = spark.table("gsl_databricks.bronze.global_superstore") \
                   .where("Sales IS NULL") \
                   .select("Row_ID", F.col("Profit").alias("profit_bronze"),
                           F.col("Shipping_Cost").alias("ship_bronze"))

fixed_rows = test.select("Row_ID", F.col("Profit").alias("profit_correcto"),
                         F.col("Shipping_Cost").alias("ship_correcto"))

bronze_rows.join(fixed_rows, "Row_ID") \
    .select(F.count("*").alias("filas"),
            F.sum((F.col("profit_bronze") != F.col("profit_correcto")).cast("int")).alias("profit_distinto"),
            F.sum((F.col("ship_bronze") != F.col("ship_correcto")).cast("int")).alias("ship_distinto")) \
    .show()