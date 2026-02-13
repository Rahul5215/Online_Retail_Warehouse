CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
RAISE NOTICE 'LOADING BRONZE LAYER';
RAISE NOTICE '==========================================';

RAISE NOTICE 'Truncating Table : bronze.online_retail';

TRUNCATE TABLE bronze.online_retail;

RAISE NOTICE 'Inserting Data Into Table : bronze.online_retail';

COPY bronze.online_retail
FROM 'C:\Online Retail Dataset\data - Online_Retail.csv'
WITH (
FORMAT CSV,
HEADER TRUE,
ENCODING 'LATIN1'
);

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


CALL bronze.load_bronze()