CREATE OR REPLACE PROCEDURE gold.load_gold()
LANGUAGE plpgsql
AS
$$
DECLARE
start_time 	   TIMESTAMP;
end_time   	   TIMESTAMP;
duration   	   INTERVAL;
tb1_start_time TIMESTAMP;
tb1_end_time   TIMESTAMP;
tb1_duration   INTERVAL;
v_state        text;
v_message      text;
v_detail 	   text;
v_hint 		   text;
BEGIN
start_time := clock_timestamp();
RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING GOLD LAYER';
RAISE NOTICE '=================================================';

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING SALES TABLE';
RAISE NOTICE '=================================================';

tb1_start_time := clock_timestamp();

RAISE NOTICE 'TRUNCATING TABLE : gold.fact_sales';

TRUNCATE TABLE gold.fact_sales;

RAISE NOTICE 'INSERTING DATA INTO TABLE : gold.fact_sales';
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
  AND unitprice IS NOT NULL;

tb1_end_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;

RAISE NOTICE '------------------------------------------------';
RAISE NOTICE 'LOADING TIME FOR SALES TABLE : %',tb1_duration;
RAISE NOTICE '------------------------------------------------';


RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING CUSTOMER TABLE';
RAISE NOTICE '=================================================';

tb1_start_time := clock_timestamp();
RAISE NOTICE 'TRUNCATING TABLE : gold.dim_customer';

TRUNCATE TABLE gold.dim_customer;

RAISE NOTICE 'INSERTING DATA INTO TABLE : gold.dim_customer';
INSERT INTO gold.dim_customer(customer_id, country)
SELECT
DISTINCT
customerid AS customer_id,
country
FROM silver.retail_cleaned;

tb1_end_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;

RAISE NOTICE '------------------------------------------------';
RAISE NOTICE 'LOADING TIME FOR CUSTOMER TABLE : %',tb1_duration;
RAISE NOTICE '------------------------------------------------';

RAISE NOTICE '=================================================';
RAISE NOTICE 'LOADING PRODUCT TABLE';
RAISE NOTICE '=================================================';

tb1_start_time := clock_timestamp();

RAISE NOTICE 'TRUNCATING TABLE : gold.dim_product';

TRUNCATE TABLE gold.dim_product;

RAISE NOTICE 'INSERTING DATA INTO TABLE : gold.dim_product';
INSERT INTO gold.dim_product(stock_code, description)
SELECT
DISTINCT
stockcode AS stock_code,
description
FROM silver.retail_cleaned;

tb1_end_time := clock_timestamp();
tb1_duration := tb1_end_time - tb1_start_time;

RAISE NOTICE '------------------------------------------------';
RAISE NOTICE 'LOADING TIME FOR PRODUCT TABLE : %',tb1_duration;
RAISE NOTICE '------------------------------------------------';

end_time := clock_timestamp();
duration := end_time - start_time;

RAISE NOTICE '=================================================';
RAISE NOTICE 'TOTAL LOADING TIME : %',duration;
RAISE NOTICE '=================================================';

EXCEPTION WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
		v_state = RETURNED_SQLSTATE,
		v_message = MESSAGE_TEXT,
		v_detail = PG_EXCEPTION_DETAIL,
		v_hint = PG_EXCEPTION_HINT;

    RAISE NOTICE 'ERROR STATE  : %', v_state;
    RAISE NOTICE 'ERROR MSG    : %', v_message;
    RAISE NOTICE 'ERROR DETAIL : %', v_detail;
    RAISE NOTICE 'ERROR HINT   : %', v_hint;


END
$$;

CALL gold.load_gold()




