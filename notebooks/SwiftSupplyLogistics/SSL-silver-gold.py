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



# COMMAND ----------



# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{config['storage_account']}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{config['storage_account']}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{config['storage_account']}.dfs.core.windows.net", dbutils.secrets.get(scope="sslscope", key="ssl-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{config['storage_account']}.dfs.core.windows.net", f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='sslscope', key='ssl-tenant-id')}/oauth2/token")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ssl_db;
# MAGIC USE ssl_db;

# COMMAND ----------

# MAGIC %md
# MAGIC dim_product

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ssl_db.dim_product (
    product_id   STRING,
    product_name STRING,
    effective_date DATE,
    source STRING
)
USING DELTA
""")

spark.sql(f"""
MERGE INTO ssl_db.dim_product AS tgt
USING (
    SELECT
        product_id,
        product_name,
        current_date()    AS effective_date,
        'inventory_silver' AS source
    FROM delta.`{paths["silver_inventory"]}`
) AS src
ON tgt.product_id = src.product_id

WHEN MATCHED AND (tgt.product_name <> src.product_name)
  THEN UPDATE SET
    tgt.product_name = src.product_name,
    tgt.effective_date = src.effective_date,
    tgt.source = src.source

WHEN NOT MATCHED
  THEN INSERT (product_id, product_name, effective_date, source)
  VALUES (src.product_id, src.product_name, src.effective_date, src.source);
""")

spark.sql("select * from dim_product;").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC dim_warehouse

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ssl_db.dim_warehouse (
    warehouse_id STRING,
    city STRING,
    effective_date DATE,
    source STRING
)
USING DELTA
""")

warehouse_mapping = [
    ("WH1", "Mumbai"),
    ("WH2", "Delhi"),
    ("WH3", "Bangalore"),
    ("WH4", "Chennai"),
    ("WH5", "Hyderabad")
]

mapping_df = spark.createDataFrame(warehouse_mapping, ["warehouse_id", "city"])
mapping_df.createOrReplaceTempView("warehouse_mapping")

spark.sql(f"""
MERGE INTO ssl_db.dim_warehouse AS tgt
USING (
    SELECT
        warehouse_id,
        city,
        current_date() AS effective_date,
        'static_mapping' AS source
    FROM warehouse_mapping
) AS src
ON tgt.warehouse_id = src.warehouse_id

WHEN MATCHED AND tgt.city <> src.city
  THEN UPDATE SET
    tgt.city = src.city,
    tgt.effective_date = src.effective_date,
    tgt.source = src.source

WHEN NOT MATCHED
  THEN INSERT (warehouse_id, city, effective_date, source)
  VALUES (src.warehouse_id, src.city, src.effective_date, src.source);
""")

spark.sql("SELECT * FROM ssl_db.dim_warehouse").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC fact_inventory_daily

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ssl_db.fact_inventory_daily (
    warehouse_id STRING,
    product_id STRING,
    quantity_in_stock INT,
    last_restock_date DATE,
    snapshot_date DATE,
    stock_status STRING
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")

spark.sql(f"""
INSERT INTO ssl_db.fact_inventory_daily
SELECT
    warehouse_id,
    product_id,
    quantity_in_stock,
    last_restock_date,
    current_date() AS snapshot_date,
    CASE WHEN quantity_in_stock < 50 THEN 'Low Stock' ELSE 'OK' END AS stock_status
FROM delta.`{paths["silver_inventory"]}`;
""")

spark.sql("SELECT * FROM ssl_db.fact_inventory_daily ORDER BY snapshot_date DESC LIMIT 10").show()


# COMMAND ----------

# MAGIC %md
# MAGIC fact_sales_history

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ssl_db.fact_sales_daily (
    warehouse_id STRING,
    product_id STRING,
    sale_date DATE,
    qty_sold BIGINT
)
USING DELTA
PARTITIONED BY (sale_date)
""")

spark.sql(f"""
MERGE INTO ssl_db.fact_sales_daily tgt
USING (
  SELECT
    warehouse_id,
    product_id,
    sale_date,
    SUM(quantity_sold) AS qty_sold
  FROM delta.`{paths["silver_salesorder"]}`
  GROUP BY warehouse_id, product_id, sale_date
) src
ON tgt.warehouse_id = src.warehouse_id
 AND tgt.product_id  = src.product_id
 AND tgt.sale_date   = src.sale_date
WHEN MATCHED THEN UPDATE SET tgt.qty_sold = src.qty_sold
WHEN NOT MATCHED THEN INSERT *;
""")


spark.sql("SELECT * FROM ssl_db.fact_sales_daily").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC fact_shipments

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ssl_db.fact_shipments (
    shipment_id STRING,
    warehouse_id STRING,
    status STRING,
    event_ts TIMESTAMP,
    event_date DATE,
    city STRING,
    delay_reason STRING,
    is_delayed BOOLEAN,
    is_recent BOOLEAN,
    latitude DOUBLE,
    longitude DOUBLE,
    source_file STRING
)
USING DELTA
PARTITIONED BY (event_date)
""")

spark.sql(f"""
MERGE INTO ssl_db.fact_shipments tgt
USING (
  WITH shipments_dedup AS (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY ingestion_time DESC) AS rn
    FROM delta.`{paths["silver_shipments"]}` s
  )
  SELECT
         shipment_id,
         warehouse_id,
         status,
         timestamp     AS event_ts,
         DATE(timestamp) AS event_date,
         city,
         delay_reason,
         is_delayed,
         is_recent,
         latitude,
         longitude,
         source_file
  FROM shipments_dedup
  WHERE rn = 1
    AND timestamp >= date_sub(current_date(), 90)
) src
ON tgt.shipment_id = src.shipment_id
   AND tgt.event_date = src.event_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
""")

spark.sql("SELECT * FROM ssl_db.fact_shipments").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC supplychainsnapshot

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ssl_db.supplychain_snapshot
(  warehouse_id STRING,
    product_id STRING,
    product_name STRING,
    quantity_in_stock INT,
    stock_status STRING,
    shipment_id STRING,
    status STRING,
    location STRING,
    timestamp TIMESTAMP,
    delay_reason STRING,
    snapshot_date DATE
)
USING DELTA
PARTITIONED BY (snapshot_date)
""")

spark.sql(f"""
INSERT OVERWRITE TABLE ssl_db.supplychain_snapshot PARTITION (snapshot_date)
WITH latest_ship AS (
  SELECT shipment_id, warehouse_id, status, city, delay_reason, event_ts,
         ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY event_ts DESC) AS rn
  FROM fact_shipments
)
SELECT
  i.warehouse_id,
  i.product_id,
  p.product_name,
  i.quantity_in_stock,
  i.stock_status,
  ls.shipment_id,
  ls.status,
  ls.city       AS location,
  ls.event_ts   AS timestamp,
  ls.delay_reason,
  current_date() AS snapshot_date
FROM ssl_db.fact_inventory_daily i
JOIN dim_product p ON i.product_id = p.product_id
JOIN (SELECT * FROM latest_ship WHERE rn = 1) ls
  ON i.warehouse_id = ls.warehouse_id
WHERE ls.status IN ('In Transit','Delayed')
  AND i.snapshot_date = current_date();
""")

spark.sql("SELECT * FROM ssl_db.supplychain_snapshot").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC alert_shipments

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS ssl_db.alerts_shipments
(   
    shipment_id STRING,
    warehouse_id STRING,
    location STRING,
    delay_reason STRING,
    timestamp TIMESTAMP,
    event_date DATE
)
USING DELTA
PARTITIONED BY (event_date)
""")

spark.sql(f"""
INSERT INTO ssl_db.alerts_shipments
SELECT
  shipment_id,
  warehouse_id,
  city AS location,
  COALESCE(delay_reason, 'Traffic') AS delay_reason,
  event_ts AS timestamp,
  DATE(event_ts) AS event_date
FROM fact_shipments
WHERE status = 'Delayed';
""")

spark.sql("SELECT * FROM ssl_db.alerts_shipments").show(5)


# COMMAND ----------

#spark.sql("show tables in ssl_db").show()





# COMMAND ----------

'''
drop table if exists ssl_db.supplychain_snapshot;
drop table if exists ssl_db.alerts_shipments;
drop table if exists ssl_db.fact_sales_daily;
drop table if exists ssl_db.fact_shipments;
drop table if exists ssl_db.fact_inventory_daily;
drop table if exists ssl_db.dim_product;
drop table if exists ssl_db.dim_warehouse;
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from supplychain_snapshot;

# COMMAND ----------

# MAGIC %sql
# MAGIC use ssl_db;
# MAGIC show tables;
# MAGIC

# COMMAND ----------

# Define the base path for ADLS
adls_base_path = base_path_gold
db_name="ssl_db"
# Write each table to ADLS as a CSV file
tables = [
    "supplychain_snapshot",
    "alerts_shipments",
    "fact_sales_daily",
    "fact_shipments",
    "fact_inventory_daily",
    "dim_product",
    "dim_warehouse"
]

for table in tables:
    print(f"{adls_base_path}/tables/{table}")
    df = spark.sql(f"SELECT * FROM `{db_name}`.`{table}`;")
    df.coalesce(1).write.option("header","true").mode("overwrite").csv(f"{adls_base_path}/tables/{table}")

# COMMAND ----------

