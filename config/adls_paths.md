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

## Naming Conventions
- Batch files: `inventory_YYYYMMDD.csv`, `salesorder_YYYYMMDD.csv`
- Shipment events: JSON with timestamp in filename
- Archive: Folders created by ingestion date (yyyyMMdd)
