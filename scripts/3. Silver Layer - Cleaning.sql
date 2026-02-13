select *from bronze.source_data

--For invoiceno column.

--checking for nulls and empty spaces.
select
invoiceno
from bronze.source_data
where invoiceno is null or trim(invoiceno) = ''

--checking for unwanted spaces.
select
invoiceno
from bronze.source_data
where invoiceno != trim(invoiceno)


select
*
from bronze.source_data
where length(invoiceno) > 6

select *from bronze.source_data
--For stockcode column.

--checking for nulls and empty spaces.
select
stockcode
from bronze.source_data
where stockcode is null or trim(stockcode) = ''

--checking for unwanted spaces.
select
stockcode
from bronze.source_data
where stockcode <> trim(stockcode)

--For description column.

--checking for nulls and empty spaces.

select
description
from bronze.source_data
where description is null or trim(description) = ''

--checking for unwanted spaces.
select
description
from bronze.source_data
where description <> trim(description)

--cleaning nulls and unwanted spaces.
select
coalesce(nullif(trim(description),''),'UNKNOWN PRODUCT')
from bronze.source_data
where description is null or trim(description) = ''


--For column quantity.
--checking for nulls.
select
quantity
from bronze.source_data
where quantity is null

--checking for invalid values
select
*
from bronze.source_data
where quantity < 1


--For invoicedate column.
--checking for nulls and empty spaces.
select
invoicedate
from bronze.source_data
where invoicedate is null or trim(invoicedate) = ''

--checking for unwanted spaces.
select
invoicedate
from bronze.source_data
where invoicedate <> trim(invoicedate)

--checking for invalid dates.
SELECT
    invoicedate
FROM bronze.source_data
WHERE TO_TIMESTAMP(invoicedate, 'MM/DD/YY HH24:MI') IS NULL;

--checking for future dates.
SELECT
    invoicedate,
    TO_TIMESTAMP(invoicedate, 'MM/DD/YY HH24:MI') AS parsed_date
FROM bronze.source_data
WHERE TO_TIMESTAMP(invoicedate, 'MM/DD/YY HH24:MI')::DATE > CURRENT_DATE;

--checking for past dates.
SELECT
    invoicedate,
	TO_TIMESTAMP(invoicedate, 'MM/DD/YY HH24:MI') AS parsed_date
FROM bronze.source_data
WHERE TO_TIMESTAMP(invoicedate, 'MM/DD/YY HH24:MI'):: DATE < '1900/01/01'


select * FROM bronze.source_data
--For unitprice column.
--check for nulls.
select
*
from bronze.source_data
where unitprice is null 

--check for negative values and zero values.
select
unitprice
from bronze.source_data
where unitprice <= 0 

--For customerid column
--check for nulls.
select
customerid
from bronze.source_data
where customerid is null

--For country column
--checking invalid country
select
distinct country
from bronze.source_data

--checking extra spaces
select
country
from bronze.source_data
where country != trim(country)






--FINAL QUERY:-
SELECT
TRIM(invoiceno) AS invoiceno,
TRIM(stockcode) AS stockcode,
coalesce(nullif(trim(description),''),'UNKNOWN PRODUCT') AS description,
quantity,
TO_TIMESTAMP(invoicedate, 'MM/DD/YY HH24:MI') AS invoicedate,
CASE WHEN unitprice <= 0 THEN NULL
     ELSE unitprice
     END 
AS unitprice,
COALESCE(customerid,-1) AS customerid,
INITCAP(TRIM(country)) AS country
from bronze.source_data
