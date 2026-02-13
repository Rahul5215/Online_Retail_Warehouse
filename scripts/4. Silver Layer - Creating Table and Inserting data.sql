CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE silver.retail_cleaned (
    invoiceno     	VARCHAR(20),
    stockcode     	VARCHAR(20),
    description   	TEXT,
    quantity      	INTEGER,
    invoicedate   	TIMESTAMP,
    unitprice     	NUMERIC(10,2),
    customerid      INTEGER,
    country         VARCHAR(100),
    is_cancelled    BOOLEAN,
    is_return 		BOOLEAN,
    is_non_product	BOOLEAN,
	load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO silver.retail_cleaned (
    invoiceno,
    stockcode,
    description,
    quantity,
    invoicedate,
    unitprice,
    customerid,
    country,
    is_cancelled,
    is_return,
    is_non_product
)
SELECT
    TRIM(invoiceno) AS invoiceno,
    TRIM(stockcode) AS stockcode,
    COALESCE(NULLIF(TRIM(description), ''), 'UNKNOWN PRODUCT') AS description,
    quantity,
    TO_TIMESTAMP(invoicedate, 'MM/DD/YY HH24:MI') AS invoicedate,
    CASE 
        WHEN unitprice <= 0 THEN NULL
        ELSE unitprice
    END AS unitprice,
    COALESCE(customerid, -1) AS customerid,
    INITCAP(TRIM(country)) AS country,
    invoiceno LIKE 'C%' AS is_cancelled,
    quantity < 0 AS is_return,
    stockcode ~ '^[A-Z]+$' AS is_non_product
FROM bronze.source_data;


SELECT *FROM silver.retail_cleaned
