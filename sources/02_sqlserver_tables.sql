-- Run in SSMS, connected to your local SQL Server Developer instance.
-- SQL Server holds the master data: customers, sellers, products, geolocation.
-- Every table has a PRIMARY KEY because Fabric mirroring needs one.
-- Enable change tracking / CDC for mirroring by following Fabric's mirroring setup steps.

IF DB_ID('RetailSource') IS NULL
    CREATE DATABASE RetailSource;
GO

USE RetailSource;
GO

DROP TABLE IF EXISTS dbo.geolocation;
DROP TABLE IF EXISTS dbo.products;
DROP TABLE IF EXISTS dbo.sellers;
DROP TABLE IF EXISTS dbo.customers;
GO

CREATE TABLE dbo.customers (
    customer_id               VARCHAR(32)   NOT NULL,
    customer_unique_id        VARCHAR(32)   NOT NULL,
    customer_zip_code_prefix  INT           NULL,
    customer_city             NVARCHAR(100) NULL,
    customer_state            CHAR(2)       NULL,
    updated_at                DATETIME2(0)  NOT NULL
        CONSTRAINT df_customers_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

CREATE TABLE dbo.sellers (
    seller_id                 VARCHAR(32)   NOT NULL,
    seller_zip_code_prefix    INT           NULL,
    seller_city               NVARCHAR(100) NULL,
    seller_state              CHAR(2)       NULL,
    updated_at                DATETIME2(0)  NOT NULL
        CONSTRAINT df_sellers_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_sellers PRIMARY KEY (seller_id)
);

-- Note: the column names product_name_lenght / product_description_lenght keep the
-- spelling mistake from the original Olist files on purpose.
CREATE TABLE dbo.products (
    product_id                   VARCHAR(32)   NOT NULL,
    product_category_name        NVARCHAR(100) NULL,
    product_name_lenght          INT           NULL,
    product_description_lenght   INT           NULL,
    product_photos_qty           INT           NULL,
    product_weight_g             INT           NULL,
    product_length_cm            INT           NULL,
    product_height_cm            INT           NULL,
    product_width_cm             INT           NULL,
    updated_at                   DATETIME2(0)  NOT NULL
        CONSTRAINT df_products_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);

-- Geolocation has no natural key (duplicates exist), so we add an identity column.
CREATE TABLE dbo.geolocation (
    geolocation_id               INT IDENTITY(1,1) NOT NULL,
    geolocation_zip_code_prefix  INT           NULL,
    geolocation_lat              FLOAT         NULL,
    geolocation_lng              FLOAT         NULL,
    geolocation_city             NVARCHAR(100) NULL,
    geolocation_state            CHAR(2)       NULL,
    CONSTRAINT pk_geolocation PRIMARY KEY (geolocation_id)
);
GO
