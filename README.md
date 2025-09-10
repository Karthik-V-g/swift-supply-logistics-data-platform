# SwiftSupply Logistics – Azure Data Platform

## 🚀 Project Overview
Build an **end-to-end Azure Data Platform** for SwiftSupply Logistics (fictional company) to handle **batch and streaming data** from retail warehouses, with **Synapse Analytics integration** for enterprise-scale reporting.

---

## 🛠️ Technologies Used
- **Azure Data Lake Storage (ADLS)**
- **Azure Databricks (PySpark, Delta Lake)**
- **Azure Data Factory (ADF)**
- **Azure Synapse Analytics** (Dedicated SQL Pool, Pipelines)
- **Azure Key Vault** (Secrets Management)
- **Python, PySpark**
- **AWS S3** (optional source storage)

---

## 📖 Introduction
SwiftSupply Logistics Pvt Ltd operates a retail & e-commerce supply chain with **5 warehouses across India** (Mumbai, Delhi, Bangalore, Chennai, Hyderabad).

The company faced:
- 📉 Stock shortages due to poor demand forecasting  
- 🚚 Customer complaints because of late shipments  
- ❌ No unified view of data — fragmented across Excel, SQL Server, IoT devices  

This project builds a **modern Azure Data Platform** that ingests, processes, curates, and warehouses batch and streaming data, enabling **real-time supply chain insights** through **Synapse Analytics**.

---

## 🏗️ Architecture
High-level architecture includes:
- **Ingestion** (S3 + IoT)  
- **Storage** (ADLS Bronze, Silver, Gold)  
- **Processing** (Databricks)  
- **Orchestration** (ADF)  
- **Warehousing** (Synapse Dedicated SQL Pool)  
- **Visualization** (Power BI via Gold layer & Synapse)  

---

## 📂 Storage Design (ADLS Containers)

archive/
├── batch/{yyyyMMdd}/ # Archived batch CSV files from S3
└── streaming/{yyyyMMdd}/ # Archived streaming JSON files from IoT

bronze/
├── batch/inventory/ # Raw inventory CSVs
├── batch/salesorder/ # Raw sales order CSVs
├── streaming/landing_shipments/ # Landing zone for shipment JSON events
└── streaming/shipments/ # Bronze Delta tables (raw streaming data)

silver/
├── batch/inventory/ # Cleaned inventory Delta tables
├── batch/salesorder/ # Cleaned sales order Delta tables
└── streaming/shipments/ # Processed shipment Delta tables

gold/
├── delta_tables/ # Curated Gold Delta tables (Databricks SQL)
├── parquet/ # Exported Parquet files (for Synapse COPY INTO)
└── _success/ # Marker file to trigger Synapse pipeline

## 📊 Data Sources
**Batch (CSV from S3)**
- `inventory`  
- `sales orders`  

**Streaming (IoT JSON from Simulator)**
- `shipments` (in-transit events)  

---

## 🔄 Pipelines & Processing

### A. Batch Pipeline (ADF)
- Triggered daily  
- Validates and ingests CSVs → Bronze  
- Orchestrates Databricks notebooks  
- Archives original files  

### B. Streaming Pipeline (Databricks)
- Autoloader notebooks clean and transform JSON → Silver  

### C. Transformation & Gold Tables (Databricks)
- Batch + streaming joins  
- Creates **7 curated Gold Delta tables**  
- Exports each Gold table as **Parquet** in ADLS `gold/parquet/`  
- Writes `_success.txt` marker when all files are ready  

**Gold Tables**
- `dim_product` (Product details)  
- `dim_warehouse` (Warehouse master)  
- `fact_inventory_daily` (Daily stock levels)  
- `fact_sales_daily` (Sales transactions)  
- `fact_shipments` (Shipment events)  
- `supplychain_snapshot` (In-transit & delayed shipments)  
- `alerts_shipments` (Delayed shipment alerts)  

### D. Synapse Analytics Integration
- **Dedicated SQL Pool** created with `gold` schema  
- Stored procedure with **COPY INTO** loads Parquet files from ADLS → Synapse tables  
- **Synapse Pipeline** monitors `gold/_success/` with **event trigger**  
- On success, pipeline executes stored procedure to refresh warehouse tables  
- **Power BI connects directly to Synapse** for reporting  

---

## ⚙️ Business Rules
- **Low Stock Rule**: `< 50` units = “Low Stock”  
- **Shipment Delay Alerts**: status = “Delayed” → insert into alerts  
- **Retention**: keep only last 90 days in curated zone  

---

## 🔐 Security
- **Azure Key Vault**: credentials for ADLS, Databricks, Synapse  
- **RBAC**:  
  - Bronze → ingestion team  
  - Silver → data engineers  
  - Gold + Synapse → BI team  

---

## 📡 Monitoring
- **ADF**: pipeline alerts  
- **Databricks**: streaming job monitoring via UI & logs  
- **Synapse**: pipeline execution logs & SQL monitoring  

---

## 📈 Power BI Dashboard
The Gold tables in **Synapse** are consumed in Power BI to deliver:
- 📦 Inventory stock snapshot  
- 🚚 Real-time shipment tracking  
- ⚠️ Delay alerts & low stock indicators  

---

## 📁 Repository Structure

swift-supply-azure-data-platform/
│── README.md
│── docs/
│ ├── SwiftSupply_Azure_Data_Platform.docx
│ ├── architecture_diagram.png
│ └── PowerBI_dashboard.png
│── data/
│ ├── inventory_sample.csv
│ ├── salesorder_sample.csv
│ └── shipments_sample.json
│── notebooks/
│ ├── SSL-batch-bronze-to-silver.py
│ ├── SSL-silver-gold.py
│ ├── SSL-streaming-bronze-silver.py
│ └── SSL-streaming-source-bronze.py
│── pipelines/
│ ├── PL_SwiftSupply_DataIngestion.json
│── scripts/
│ └── Source_files_generator.py
│── configs/
│ ├── keyvault_config.json
│ └── adls_paths.md
└── powerbi/
└── supplychain_dashboard.pbix


---

## 🛠️ Setup Guide
1. Deploy **ADLS Gen2 containers** (archive, bronze, silver, gold)  
2. Configure **Azure Key Vault** with credentials  
3. Deploy **ADF pipelines**  
4. Upload sample files to S3  
5. Run shipment simulator  
6. Trigger ADF pipeline → Bronze → Silver  
7. Databricks notebooks → Gold (Delta + Parquet)  
8. `_success.txt` triggers **Synapse Pipeline**  
9. Stored procedure loads data into **Synapse SQL Pool**  
10. Connect **Power BI to Synapse**  

---

## 🔮 Next Enhancements
- 🤖 Machine Learning demand forecasting with **Azure ML**  
- ✅ Data Quality Monitoring with **Delta Live Tables** or **Great Expectations**  
- 🔄 CI/CD pipelines for **ADF, Databricks, and Synapse** 
