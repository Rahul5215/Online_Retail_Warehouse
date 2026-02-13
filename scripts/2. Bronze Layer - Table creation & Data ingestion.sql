DROP TABLE IF EXISTS bronze.online_retail;
CREATE TABLE bronze.online_retail (
    invoiceno     VARCHAR(20),
    stockcode     VARCHAR(20),
    description   TEXT,
    quantity      INTEGER,
    invoicedate   TEXT,
    unitprice     NUMERIC(10, 2),
    customerid    INTEGER,
    country       VARCHAR(100)
);

COPY bronze.online_retail
FROM 'C:\Online Retail Dataset\data - Online_Retail.csv'
WITH (
FORMAT CSV,
HEADER TRUE,
ENCODING 'LATIN1'
)

SELECT * FROM bronze.online_retail
