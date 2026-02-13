select *from silver.retail_cleaned

--fact sales table
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

--dimension customer table
SELECT
DISTINCT
customerid AS customer_id,
country
FROM silver.retail_cleaned

--dimension product table
SELECT
DISTINCT
stockcode AS stock_code,
description,
unitprice AS unit_price
FROM silver.retail_cleaned




