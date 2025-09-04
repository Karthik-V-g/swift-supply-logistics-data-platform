# SwiftSupply Logistics – Azure Data Platform

## Project Overview
Build an end-to-end Azure Data Platform for SwiftSupply Logistics (fictional company) to handle batch and streaming data from retail warehouses.

## Technologies Used
    - Azure Data Lake Storage (ADLS)
    - Azure Databricks (PySpark, Delta Lake)
    - Azure Data Factory (ADF)
    - Key Vault (Secrets Management)
    - Python, PySpark
    - Power BI (BI layer)
    - AWS S3 (optional source storage)
- 
## 1. Introduction
      SwiftSupply Logistics Pvt Ltd operates a retail & e-commerce supply chain with 5 warehouses across India (Mumbai, Delhi, Bangalore, Chennai, Hyderabad).
      The company faced:
                * Stock shortages due to poor demand forecasting.
                * Customer complaints because of late shipments.
                * No unified view of data — spread across Excel, SQL Server, IoT devices.
      This project builds a modern Azure Data Platform that ingests, processes, and curates batch and streaming data, enabling real-time supply chain insights through Power BI.

## 2. Architecture
   High-level architecture showing ingestion (S3 + IoT), storage (ADLS), processing (Databricks), orchestration (ADF), and visualization (Power BI).

## 3. Storage Design (ADLS Containers)
      archive/
        ├── batch/{date}/
        └── streaming/{date}/
      
      bronze/
        ├── batch/inventory/
        ├── batch/salesorder/
        ├── streaming/landing_shipments/
        └── streaming/shipments/
      
      silver/
        ├── batch/inventory/
        ├── batch/salesorder/
        └── streaming/shipments/

gold/
  └── delta_tables/ (managed by Databricks SQL)

## 4. Data Sources
## Batch (Historical CSVs – from S3)
## Inventory
    Columns: warehouse_id, product_id, product_name, category, quantity_in_stock, last_restock_date
    Example:
    WH1, PRD001, Laptop Bag, Accessories, 120, 2025-07-15

## Sales Orders
    Columns: order_id, product_id, quantity_sold, sale_date, warehouse_id
    Example:
    ORD001, PRD001, 5, 2025-07-10, WH1

## Streaming (IoT JSON – from Simulator)
    Schema:
    
    {
      "shipment_id": "SH92384",
      "warehouse_id": "WH1",
      "status": "In Transit",
      "location": "Mumbai",
      "timestamp": "2025-08-12T10:25:00Z"
    }

## 5. Pipelines & Processing
   ## A. Batch Pipeline (ADF)
      * Triggered daily.
      * Monitors S3 bucket for inventory_<date>.csv and salesorder_<date>.csv.
      * Waits up to 2 hours for both files.
      * If files exist → copied to bronze/batch/.
      * If missing/invalid → pipeline times out & sends alert.
      * After ingestion → runs Databricks notebooks for processing.
      * Archives files to archive/batch/{date}/.

   ## B. Streaming Pipeline (Databricks)
      * Python script generates shipment JSON events → stored in bronze/streaming/landing_shipments/.
      * Autoloader Notebook 1
      * Reads JSONs from landing.
      * Cleans & writes Delta → bronze/streaming/shipments/.
      * Archives processed JSONs.
      * Autoloader Notebook 2
      * Reads Bronze shipments.
      * Transforms → writes Delta → silver/streaming/shipments/.

   ## C. Transformation & Gold Tables
      * Notebook 1 – Batch Transform
      * Cleans batch CSVs:
      * Drop nulls.
      * Convert dates → yyyy-MM-dd.
      * Cast numeric columns.
      * Saves as Silver Delta.
      * Notebook 2 – Gold Tables Creation
      * Joins batch & streaming Silver data.
      * Creates 7 curated Delta tables:
            Table Name (Description)
            dim_product	(Product details)
            dim_warehouse	(Warehouse master)
            fact_inventory_daily (Daily stock levels)
            fact_sales_daily	(Sales transactions)
            fact_shipments	(Shipment events)
            supplychain_snapshot	(In-transit & delayed shipments)
            alerts_shipments	(Delayed shipment alerts)

## 6. Business Rules
   ## Low Stock Rule
      If quantity_in_stock < 50 → mark product as "Low Stock".
   ## Shipment Delay Alerts
      If status = 'Delayed' → log in alerts_shipments with reason "Traffic" or "Weather".
   ## Retention Rule
      Keep only last 90 days of shipment data in curated zone.

## 7. Security
      Azure Key Vault: 
                       Stores ADLS, Databricks credentials.
      ADLS RBAC:
                        Bronze → ingestion team.
                        Silver → data engineers.
                        Gold → BI/analytics team.

## 8. Monitoring
      ADF: Failure or timeout → email alert.
      Databricks: Streaming job monitoring via UI & logs.

## 9. Power BI Dashboard
      * The Gold tables are consumed in Power BI to deliver:
      * Inventory stock snapshot.
      * Real-time shipment tracking.
      * Delay alerts & low stock indicators.

## 10. Repository Structure
        swift-supply-azure-data-platform/
        │── README.md
        │── docs/
        │   ├── SwiftSupply_Azure_Data_Platform.docx
        │   ├── architecture_diagram.png
        │   └── PowerBI_dashboard.png
        │── data/
        │   ├── inventory_sample.csv
        │   ├── salesorder_sample.csv
        │   └── shipments_sample.json
        │── notebooks/
        │   ├── SSL-batch-bronze-to-silver.py
        │   ├── SSL-silver-gold.py
        │   ├── SSL-streaming-bronze-silver.py
        │   └── SSL-streaming-source-bronze.py
        │── pipelines/
        │   ├── PL_SwiftSupply_DataIngestion.json 
        │── scripts/
        │   └── Source_files_generator.py
        │── configs/
        │   ├── keyvault_config.json
        │   └── adls_paths.md
        └── powerbi/
            └── supplychain_dashboard.pbix

##  11. Setup Guide
        * Deploy ADLS Gen2 and create containers (archive, bronze, silver, gold).
        * Configure Azure Key Vault with ADLS and Databricks credentials.
        * Deploy ADF pipeline from JSON in /pipelines.
        * Upload sample files from /data to S3 bucket.
        * Run shipment simulator (shipment_simulator.py).
        * Trigger ADF pipeline → validates & ingests batch files.
        * Databricks notebooks run automatically via ADF.
        * Verify Gold tables in Databricks SQL.
        * Connect Power BI to Gold layer using /powerbi/supplychain_dashboard.pbix.

## 12. Next Enhancements
        * Machine Learning demand forecasting with Azure ML.
        * Data Warehouse integration via Azure Synapse.
        * CI/CD pipelines for ADF & Databricks.
        * Data Quality Monitoring using Delta Live Tables or Great Expectations.
