# SwiftSupply Logistics Data Platform
An end to end Supply chain ETL Project built using Azure services.

## Project Overview
Build an end-to-end Azure Data Platform for SwiftSupply Logistics (fictional) to handle batch and streaming data from retail warehouses.

## Technologies Used
- Azure Data Lake Storage (ADLS)
- Azure Databricks (PySpark, Delta Lake)
- Azure Data Factory (ADF)
- Event Hub (IoT Streaming)
- Key Vault (Secrets Management)
- Python, PySpark
- Power BI (BI layer)
- AWS S3 (optional source storage)

## Project Structure
- **src/**: Contains notebooks, scripts, and config files for batch & streaming pipelines.
- **data/**: Raw, processed, and curated datasets (CSV, JSON, Delta).
- **pipelines/**: JSON exports of ADF pipelines.
- **tests/**: Unit and integration tests.
- **docs/**: Project documentation.
