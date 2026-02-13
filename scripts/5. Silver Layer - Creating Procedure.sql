CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS
$$
DECLARE 
start_time  TIMESTAMP;
end_time 	TIMESTAMP;
duration	INTERVAL;
v_state     text;
v_message   text;
v_detail 	text;
v_hint 		text;
BEGIN
start_time:= clock_timestamp();

RAISE NOTICE '==========================================';
RAISE NOTICE 'LOADING SILVER LAYER';
RAISE NOTICE '==========================================';

RAISE NOTICE 'Truncating Table : silver.retail_cleaned';

TRUNCATE TABLE silver.retail_cleaned;

RAISE NOTICE 'Inserting Data Into Table : silver.retail_cleaned';

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

end_time:= clock_timestamp();
duration:= end_time - start_time;

RAISE NOTICE '------------------------------------------';
RAISE NOTICE 'TOTAL LOADING TIME :%',duration;
RAISE NOTICE '------------------------------------------';

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


CALL silver.load_silver()