# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC app id
# MAGIC
# MAGIC edc7242d-6ee7-49e8-87f9-37e71582294a
# MAGIC
# MAGIC obj id
# MAGIC
# MAGIC 2c524a1f-12bf-4562-abbc-753974ed099a
# MAGIC
# MAGIC tenant id
# MAGIC
# MAGIC b4aced7a-e53c-42c4-b32c-0fdfe808f677
# MAGIC
# MAGIC value
# MAGIC 1qw8Q~jV_KBDP5ybu2VPqaBi6Z_KMNAo_LGqocrf
# MAGIC
# MAGIC secret id
# MAGIC 0b15c5b0-c998-4d59-9549-d974299c8748
# MAGIC
# MAGIC databricksaccesstoken
# MAGIC dapi25ce32d1a2c5c5f937ef8fcfad081fa8

# COMMAND ----------

#dbutils.fs.ls("abfss://ssl-bronze@datalakessl.dfs.core.windows.net/batch/inventory")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime

# Config dictionary
config = {
    "storage_account": "datalakessl",
    "container_bronze": "ssl-bronze",
    "container_silver": "ssl-silver", 
    "source_system" : "aws-s3"
}
    

# Base path in ADLS
base_path_bronze = f"abfss://{config['container_bronze']}@{config['storage_account']}.dfs.core.windows.net"
base_path_silver = f"abfss://{config['container_silver']}@{config['storage_account']}.dfs.core.windows.net"

paths = {
    "bronze_inventory": f"{base_path_bronze}/batch/inventory",
    "bronze_salesorder": f"{base_path_bronze}/batch/salesorder",
    "silver_inventory": f"{base_path_silver}/batch/inventory",
    "silver_salesorder": f"{base_path_silver}/batch/salesorder"
}

current_date = datetime.now().strftime("%Y%m%d")

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{config['storage_account']}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{config['storage_account']}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{config['storage_account']}.dfs.core.windows.net", f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='sslscope', key='ssl-tenant-id')}/oauth2/token")


# COMMAND ----------

# MAGIC %md INVENTORY

# COMMAND ----------

inventory_schema = StructType([
    StructField("warehouse_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("quantity_in_stock", IntegerType()),
    StructField("last_restock_date", StringType())
])



inventory_df = (
    spark.read.format("csv")
    .schema(inventory_schema)
    .option("header", True)
    .load(f"{paths['bronze_inventory']}/inventory_{current_date}.csv")
)

# Data cleaning (drop null records, fill missing values, schema enforcement, normalization)
inventory_df2 = (
    inventory_df
    .dropna(subset=["product_id", "product_name"])
    .fillna({"quantity_in_stock": 0})
    .withColumn("quantity_in_stock", col("quantity_in_stock").cast(IntegerType()))
    .withColumn("last_restock_date", to_date(col("last_restock_date"), "M/d/yyyy"))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("warehouse_id", upper(col("warehouse_id")))
    .withColumn("product_id", upper(col("product_id")))
)

# Deduplicate by latest last_restock_date
window = Window.partitionBy("warehouse_id", "product_id").orderBy(col("last_restock_date").desc())
inventory_df3 = inventory_df2.withColumn("duplicate_rank", row_number().over(window)).filter(col("duplicate_rank") == 1).drop("duplicate_rank")

# Add audit columns
inventory_df4 = inventory_df3.withColumn("ingestion_timestamp", lit(datetime.utcnow()).cast(TimestampType())) \
                           .withColumn("source_system", lit(config["source_system"]))

# Save with partitioning
(inventory_df4.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("warehouse_id")  
    .option("overwriteSchema", "true")
    .save(paths["silver_inventory"]))


# COMMAND ----------

inventory_df4.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC SALESORDER

# COMMAND ----------

sales_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity_sold", IntegerType(), True),
    StructField("sale_date", DateType(), True),
    StructField("warehouse_id", StringType(), True)
])

sales_df = (
    spark.read.format("csv")
    .schema(sales_schema)
    .option("header", True)
    .load(f"{paths['bronze_salesorder']}/salesorder_{current_date}.csv")
)

# Clean (fill missing values, schema enforcement, drop null records)
sales_df2 = (
    sales_df
    .fillna({"quantity_sold": 0})
    .dropna(subset=["order_id", "product_id", "warehouse_id"])
    .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
    .withColumn("quantity_sold", col("quantity_sold").cast(IntegerType()))
)

# Deduplicate by latest sale_date
window = Window.partitionBy("order_id", "product_id", "warehouse_id").orderBy(col("sale_date").desc())
sales_df3 = sales_df2.withColumn("RN", row_number().over(window)).filter(col("RN") == 1).drop("RN")

# Add audit columns
sales_df4 = sales_df3.withColumn("ingestion_timestamp", lit(datetime.utcnow()).cast(TimestampType())) \
                   .withColumn("source_system", lit(config["source_system"]))

# Save with partitioning
(sales_df4.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("warehouse_id")
    .option("overwriteSchema", "true")
    .save(paths["silver_salesorder"]))


# COMMAND ----------

sales_df4.show(3)

# COMMAND ----------

