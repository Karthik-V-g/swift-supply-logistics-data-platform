CREATE PROCEDURE dbo.Gold_Data_Ingestion
AS
BEGIN
    
--IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
--BEGIN   
--    CREATE SCHEMA gold;
--END;

---dim_product---

IF NOT EXISTS (
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'dim_product' AND s.name = 'gold'
)
BEGIN       
    CREATE TABLE gold.dim_product
    (
       product_id     NVARCHAR(50),
       product_name   NVARCHAR(200),
       effective_date DATE,
       source         NVARCHAR(100)
    )
    WITH (DISTRIBUTION = ROUND_ROBIN);
END;

TRUNCATE TABLE gold.dim_product;

COPY INTO gold.dim_product
FROM 'https://datalakessl.dfs.core.windows.net/ssl-gold/dim_product_parquet/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY= 'Managed Identity'),
    MAXERRORS = 0
);

---dim_product---

IF NOT EXISTS (
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'dim_warehouse' AND s.name = 'gold'
)
BEGIN       
    CREATE TABLE gold.dim_warehouse
    (  
        warehouse_id     NVARCHAR(200),
        city             NVARCHAR(200),
        effective_date            DATE,
        source           NVARCHAR(200)
       
    )
    WITH (DISTRIBUTION = ROUND_ROBIN);
END;

TRUNCATE TABLE gold.dim_warehouse;

COPY INTO gold.dim_warehouse
FROM 'https://datalakessl.dfs.core.windows.net/ssl-gold/dim_warehouse_parquet/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY= 'Managed Identity'),
    MAXERRORS = 0
);


---fact_inventory_daily---

IF NOT EXISTS (
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'fact_inventory_daily' AND s.name = 'gold'
)
BEGIN       
    CREATE TABLE gold.fact_inventory_daily
    (
       warehouse_id     NVARCHAR(200),
       product_id       NVARCHAR(200),
       quantity_in_stock          INT,
       last_restock_date         DATE,
       snapshot_date             DATE,
       stock_status      NVARCHAR(200)
    )
    WITH (DISTRIBUTION = ROUND_ROBIN);
END;

TRUNCATE TABLE gold.fact_inventory_daily;

COPY INTO gold.fact_inventory_daily
FROM 'https://datalakessl.dfs.core.windows.net/ssl-gold/fact_inventory_daily_parquet/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY= 'Managed Identity'),
    MAXERRORS = 0
);

---fact_sales_daily---

IF NOT EXISTS (
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'fact_sales_daily' AND s.name = 'gold'
)
BEGIN       
    CREATE TABLE gold.fact_sales_daily
    (
       warehouse_id  NVARCHAR(200),
       product_id    NVARCHAR(200),
       sale_date              DATE,
       qty_sold             BIGINT

    )
    WITH (DISTRIBUTION = ROUND_ROBIN);
END;

TRUNCATE TABLE gold.fact_sales_daily;

COPY INTO gold.fact_sales_daily
FROM 'https://datalakessl.dfs.core.windows.net/ssl-gold/fact_sales_daily_parquet/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY= 'Managed Identity'),
    MAXERRORS = 0
);

---fact_shipments---

IF NOT EXISTS (
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'fact_shipments' AND s.name = 'gold'
)
BEGIN       
    CREATE TABLE gold.fact_shipments
    (
       shipment_id NVARCHAR(200),
       warehouse_id NVARCHAR(200),
       status  NVARCHAR(200),
       event_ts DATETIME,
       event_date DATE,
       city NVARCHAR(200),
       delay_reason NVARCHAR(200),
       is_delayed BIT,
       is_recent BIT,
       latitude FLOAT,
       longitude FLOAT,
       source_file NVARCHAR(200)

    )
    WITH (DISTRIBUTION = ROUND_ROBIN);
END;

TRUNCATE TABLE gold.fact_shipments;

COPY INTO gold.fact_shipments
FROM 'https://datalakessl.dfs.core.windows.net/ssl-gold/fact_shipments_parquet/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY= 'Managed Identity'),
    MAXERRORS = 0
);

---alerts_shipments---

IF NOT EXISTS (
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'alerts_shipments' AND s.name = 'gold'
)
BEGIN       
    CREATE TABLE gold.alerts_shipments
    (
       shipment_id NVARCHAR(200),
       warehouse_id NVARCHAR(200),
       location  NVARCHAR(200),
       delay_reason NVARCHAR(200),
       [timestamp] DATETIME,
       event_date DATE
    )
    WITH (DISTRIBUTION = ROUND_ROBIN);
END;

TRUNCATE TABLE gold.alerts_shipments;

COPY INTO gold.alerts_shipments
FROM 'https://datalakessl.dfs.core.windows.net/ssl-gold/alerts_shipments_parquet/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY= 'Managed Identity'),
    MAXERRORS = 0
);

---supplychain_snapshot---

IF NOT EXISTS (
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'supplychain_snapshot' AND s.name = 'gold'
)
BEGIN       
    CREATE TABLE gold.supplychain_snapshot
    (
       warehouse_id NVARCHAR(200),
       product_id NVARCHAR(200),
       product_name NVARCHAR(200),
       quantity_in_stock INT,
       stock_status NVARCHAR(200),
       shipment_id NVARCHAR(200),
       status NVARCHAR(200),
       location NVARCHAR(200),
       timestamp DATETIME,
       delay_reason NVARCHAR(200),
       snapshot_date DATE
    )
    WITH (DISTRIBUTION = ROUND_ROBIN);
END;

TRUNCATE TABLE gold.supplychain_snapshot;

COPY INTO gold.supplychain_snapshot
FROM 'https://datalakessl.dfs.core.windows.net/ssl-gold/supplychain_snapshot_parquet/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY= 'Managed Identity'),
    MAXERRORS = 0
);

END;
GO
