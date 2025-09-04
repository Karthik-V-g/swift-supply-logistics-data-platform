# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime

# Config dictionary
config = {
    "storage_account": "datalakessl",
    "container_bronze": "ssl-bronze",
    "container_silver": "ssl-silver",
    "source_system" : "TMS_live"
}
    

# Base path in ADLS
base_path_bronze = f"abfss://{config['container_bronze']}@{config['storage_account']}.dfs.core.windows.net"
base_path_silver = f"abfss://{config['container_silver']}@{config['storage_account']}.dfs.core.windows.net"

paths = {
    "bronze_landing_shipments": f"{base_path_bronze}/streaming/landing_shipments",
    "bronze_shipments": f"{base_path_bronze}/streaming/shipments",
    "bronze_shipments_checkpoints": f"{base_path_bronze}/streaming/shipments_checkpoints",
    "bronze_shipments_autoloader_schema": f"{base_path_bronze}/streaming/shipments_autoloader_schema", 
    "silver_shipments": f"{base_path_silver}streaming/shipments",
    "silver_shipments_checkpoints": f"{base_path_silver}/streaming/shipments_checkpoints"
}

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{config['storage_account']}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{config['storage_account']}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{config['storage_account']}.dfs.core.windows.net", f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='sslscope', key='ssl-tenant-id')}/oauth2/token")


# COMMAND ----------

# Landing/Raw JSON schema (loosely typed)
schema_raw = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("location", StringType(), True),   
    StructField("timestamp", StringType(), True),   
    StructField("delay_reason", StringType(), True),
])

shipments_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", paths["bronze_shipments_autoloader_schema"])
         .option("cloudFiles.includeExistingFiles", "true")    
         .option("rescuedDataColumn", "_rescued_data")  
         .option("multiline","true")      
         .schema(schema_raw)                                  
         .load(paths["bronze_landing_shipments"])
)

# Standardize types, Normalize keys & trim, Source/audit columns
shipments_df2 = (
    shipments_df
    .withColumn("timestamp", to_timestamp("timestamp"))
    .withColumn("warehouse_id", upper(trim(col("warehouse_id"))))
    .withColumn("shipment_id", upper(trim(col("shipment_id"))))
    .withColumn("status", lower(trim(col("status"))))
    .withColumn("delay_reason", trim(col("delay_reason"))) 
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("source_system", lit(config["source_system"]))
)

query_shipments_bronze = (
    shipments_df2.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", paths["bronze_shipments_checkpoints"])
        .start(paths["bronze_shipments"])
)

query_shipments_bronze.awaitTermination()

# COMMAND ----------

#query_shipments_bronze.isActive

# COMMAND ----------

#spark.read.format("delta").load(paths["bronze_shipments"]).printSchema()

# COMMAND ----------

#spark.read.format("delta").load(paths["bronze_shipments"]).show(3,truncate=False)

# COMMAND ----------

