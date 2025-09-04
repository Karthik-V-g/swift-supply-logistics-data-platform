# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime

# Config dictionary
config = {
    "storage_account": "datalakessl",
    "container_silver": "ssl-silver",
    "container_gold": "ssl-gold"
}
    

# Base path in ADLS
base_path_silver = f"abfss://{config['container_silver']}@{config['storage_account']}.dfs.core.windows.net"
base_path_gold = f"abfss://{config['container_gold']}@{config['storage_account']}.dfs.core.windows.net"


paths = {
    "silver_inventory"         : f"{base_path_silver}/batch/inventory",
    "silver_salesorder"        : f"{base_path_silver}/batch/salesorder",
    "silver_shipments"         : f"{base_path_silver}/streaming/shipments",
    "gold_dim_product"         : f"{base_path_gold}/tables/dimproduct",
    "gold_dim_warehouse"       : f"{base_path_gold}/tables/dimwarehouse",
    "gold_fact_inventory_daily" : f"{base_path_gold}/tables/factinventorydaily",
    "gold_fact_sales_daily"    : f"{base_path_gold}/tables/factsalesdaily",
    "gold_fact_shipments"      : f"{base_path_gold}/tables/factshipments",
    "gold_supplychain_snapshot": f"{base_path_gold}/tables/supplychainsnapshot",
    "gold_alerts_shipments"    : f"{base_path_gold}/tables/alertsshipments"
}

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{config['storage_account']}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{config['storage_account']}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{config['storage_account']}.dfs.core.windows.net", f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='sslscope', key='ssl-tenant-id')}/oauth2/token")

# COMMAND ----------



# COMMAND ----------


stream2=spark.read.format("delta").load("abfss://ssl-silver@datalakessl.dfs.core.windows.net/streaming/shipments")
print(stream2.count())
#stream2.printSchema()

stream=spark.read.format("delta").load("abfss://ssl-bronze@datalakessl.dfs.core.windows.net/streaming/shipments")
print(stream.count())
#stream.printSchema()
#stream.orderBy(col("shipment_id").desc()).show(100)




# COMMAND ----------

# Get the list of active streaming queries
active_streams = spark.streams.active

# Display information about each active stream
for stream in active_streams:
    print(f"Name: {stream.name}")
    print(f"ID: {stream.id}")
    print(f"Status: {stream.status}")
    print(f"Source: {stream.source}")
    print(f"Sink: {stream.sink}")
    print("-" * 40)

# COMMAND ----------

