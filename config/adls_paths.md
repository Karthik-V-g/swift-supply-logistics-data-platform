# ADLS Path Convention – SwiftSupply Data Platform

## Root Containers
- archive
- ssl-bronze
- ssl-silver
- ssl-gold

## Folder Structure
- archive/
  - batch/{yyyyMMdd}/ (csv files)
  - streaming/{yyyyMMdd}/ (json files)

- bronze/
  - batch/
    - filemapping/fileMapping.json  (used by lookup task)
    - inventory/ (csv file)
    - salesorder/ (csv file)
  - streaming/
    - landing_shipments/ (json files)
    - shipments/ (delta files)
    - shipments_autoloader_schema/
    - shipments_checkpoints/

- silver/
  - batch/
    - inventory/ (delta files)
    - salesorder/ (delts files)
  - streaming/
    - shipments/ (delta files)
    - shipments/checkpoints

- gold/
  - tables/ (7 delta tables managed by Databricks SQL)
  - dim_product_parquet/ (parquet file from delta table)
  - dim_warehouse_parquet/ (parquet file from delta table)
  - fact_inventory_daily_parquet / (parquet file from delta table)
  - fact_sales_daily_parquet / (parquet file from delta table)
  - fact_shipments_parquet/ (parquet file from delta table)
  - supplychain_snapshot_parquet/ (parquet file from delta table)
  - alerts_shipments_parquet/ (parquet file from delta table)
  - _success/ (acknowledgement txt file for 7 parquet files)
        
## Naming Conventions
- Batch files: `inventory_YYYYMMDD.csv`, `salesorder_YYYYMMDD.csv`
- Shipment events: JSON with timestamp in filename
- Archive: Folders created by ingestion date (yyyyMMdd)

