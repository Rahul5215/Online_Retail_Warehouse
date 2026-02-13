CREATE SCHEMA IF NOT EXISTS gold;

-----------------------------------------------------------------
--------------------SALES TABLE--------------------------------
-----------------------------------------------------------------

CREATE TABLE gold.fact_sales (
    invoice_number   VARCHAR(20),
    stock_code       VARCHAR(20),
    customer_id      INTEGER,
    invoice_date     DATE,
    quantity         INTEGER,
    unit_price       NUMERIC(10,2),
    revenue          NUMERIC(12,2)
);

INSERT INTO gold.fact_sales(invoice_number, stock_code, customer_id, invoice_date, quantity, unit_price, revenue)
SELECT
invoiceno AS invoice_number,
stockcode AS stock_code,
customerid AS customer_id,
invoicedate::date AS invoice_date,
quantity,
unitprice AS unit_price,
(unitprice * quantity) AS revenue
FROM silver.retail_cleaned
WHERE is_cancelled = false
  AND is_return = false
  AND is_non_product = false
  AND unitprice IS NOT NULL


-----------------------------------------------------------------
--------------------CUSTOMER TABLE-------------------------------
-----------------------------------------------------------------

CREATE TABLE gold.dim_customer(
customer_id INT,
country VARCHAR(30)
);

INSERT INTO gold.dim_customer(customer_id, country)
SELECT
DISTINCT
customerid AS customer_id,
country
FROM silver.retail_cleaned


-----------------------------------------------------------------
--------------------PRODUCT TABLE--------------------------------
-----------------------------------------------------------------

CREATE TABLE gold.dim_product(
stock_code VARCHAR(50),
description TEXT
);

INSERT INTO gold.dim_product(stock_code, description)
SELECT
DISTINCT
stockcode AS stock_code,
description
FROM silver.retail_cleaned

