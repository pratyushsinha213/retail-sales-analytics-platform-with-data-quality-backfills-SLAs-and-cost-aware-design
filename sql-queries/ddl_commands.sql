CREATE EXTERNAL DATA SOURCE gold_datalake
WITH (
    LOCATION = 'your_storage_account_url/{gold_path}',
);

CREATE EXTERNAL FILE FORMAT parquet_format
WITH (
    FORMAT_TYPE = PARQUET
);

CREATE EXTERNAL TABLE fact_sales (
    sale_id NVARCHAR(50),
    sale_date DATE,
    store_key INT,
    product_key INT,
    category_key INT,
    quantity INT,
    price INT,
    revenue INT,
    has_claim INT
)
WITH (
    LOCATION = 'fact_sales/',
    DATA_SOURCE = gold_datalake,
    FILE_FORMAT = parquet_format
);

CREATE EXTERNAL TABLE dim_product (
    product_key INT,
    product_id NVARCHAR(50),
    product_name NVARCHAR(200),
    category_id NVARCHAR(50),
    launch_date DATE,
    price INT
)
WITH (
    LOCATION = 'dim_product/',
    DATA_SOURCE = gold_datalake,
    FILE_FORMAT = parquet_format
);

CREATE EXTERNAL TABLE dim_category (
    category_key INT,
    category_id NVARCHAR(50),
    category_name NVARCHAR(100)
)
WITH (
    LOCATION = 'dim_category/',
    DATA_SOURCE = gold_datalake,
    FILE_FORMAT = parquet_format
);

CREATE EXTERNAL TABLE dim_store (
    store_key INT,
    store_id NVARCHAR(50),
    store_name NVARCHAR(200),
    city NVARCHAR(100),
    country NVARCHAR(100)
)
WITH (
    LOCATION = 'dim_store/',
    DATA_SOURCE = gold_datalake,
    FILE_FORMAT = parquet_format
);


-- DROP EXTERNAL TABLE dim_category;
-- DROP EXTERNAL TABLE dim_product;
-- DROP EXTERNAL TABLE dim_store;
-- DROP EXTERNAL TABLE fact_sales;
-- DROP EXTERNAL FILE FORMAT parquet_format;
-- DROP EXTERNAL DATA SOURCE gold_datalake;