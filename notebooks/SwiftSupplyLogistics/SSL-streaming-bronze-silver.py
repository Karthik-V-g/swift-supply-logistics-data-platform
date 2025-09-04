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
    "silver_shipments": f"{base_path_silver}/streaming/shipments",
    "silver_shipments_checkpoints": f"{base_path_silver}/streaming/shipments_checkpoints"
}

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{config['storage_account']}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{config['storage_account']}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{config['storage_account']}.dfs.core.windows.net", f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='sslscope', key='ssl-tenant-id')}/oauth2/token")

# COMMAND ----------

shipments_silver_df = (
    spark.readStream
         .format("delta") 
         .load(paths["bronze_shipments"])
)

# watermark on ingestion_time, drop duplicates by shipment_id keeping latest event_time
# Standardize status, Enforce/clean fields, Parse location (lat,long) vs city
shipments_silver_df2 = (
    shipments_silver_df
        .withWatermark("ingestion_time", "5 minutes")
        .dropDuplicates(["shipment_id"])  
        .filter(col("shipment_id").isNotNull() & col("warehouse_id").rlike("^WH[0-9]+$")
        )      
        .withColumn("status",
            when(col("status").like("%transit%"),   lit("In Transit"))
          .when(col("status").like("%delayed%"),   lit("Delayed"))
          .when(col("status").like("%delivered%"), lit("Delivered"))
          .otherwise(lit("Unknown"))
        )    
        .withColumn("is_delayed", col("status") == "Delayed")
        .withColumn("is_recent",  col("timestamp") >= expr("current_timestamp() - INTERVAL 1 DAY"))
        .withColumn("latitude",
            when(col("location").rlike("^[0-9.+-]+,[0-9.+-]+$"),
                 split(col("location"), ",").getItem(0).cast("double")))
        .withColumn("longitude",
            when(col("location").rlike("^[0-9.+-]+,[0-9.+-]+$"),
                 split(col("location"), ",").getItem(1).cast("double")))
        .withColumn("city",
            when(~col("location").rlike("^[0-9.+-]+,[0-9.+-]+$"),
                 initcap(trim(col("location")))))
        .drop("location")
)

query_shipments_silver = (
    shipments_silver_df2.writeStream
        .format("delta")
        .option("checkpointLocation", paths["silver_shipments_checkpoints"])
        .outputMode("append")
        .partitionBy("warehouse_id")
        .start(paths["silver_shipments"])
)

query_shipments_silver.awaitTermination()



# COMMAND ----------

#query_shipments_silver.stop()

# COMMAND ----------

