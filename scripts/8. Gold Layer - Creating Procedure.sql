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


select *
from gold.dim_customer

select *
from gold.dim_product

select *
from gold.fact_sales

--Top 5 products by revenue
with cte as(
select
p.description,
sum(s.revenue) as total_revenue
from gold.fact_sales s
left join gold.dim_product p
on p.stock_code = s.stock_code
group by p.description
)
,cte2 as(
select 
*,
dense_rank() over(order by total_revenue desc) as ranking
from cte
)
select 
description as product_name,
total_revenue
from cte2 
where ranking <= 5

--Top 5 least performing products
with cte as(
select
p.description,
sum(s.revenue) as total_revenue
from gold.fact_sales s
left join gold.dim_product p
on p.stock_code = s.stock_code
group by p.description
)
,cte2 as(
select 
*,
dense_rank() over(order by total_revenue asc) as ranking
from cte
)
select 
description as product_name,
total_revenue
from cte2 
where ranking <= 5

--Top 5 most sold products
select
p.stock_code,
p.description,
sum(s.quantity) as total_quantity_sold
from gold.fact_sales s
left join gold.dim_product p
on s.stock_code = p.stock_code
group by p.stock_code, p.description
order by total_quantity_sold desc limit 

--Unsold products
select
p.stock_code,
p.description
from gold.dim_product p
left join gold.fact_sales s
on s.stock_code = p.stock_code
where s.stock_code is null
order by p.stock_code

--Customer Revenue Segmentation Analysis
with cte as(
select
customer_id,
sum(revenue) as total_revenue
from gold.fact_sales
group by customer_id
order by sum(revenue) desc
)
,cte2 as(
select 
*,
ntile(5) over(order by total_revenue) as bucket
from cte
)
select
customer_id,
total_revenue,
case when bucket = 1 then 'low-paying-customer'
     when bucket in(2,3,4) then 'low-paying-customer'
	 else 'high-paying-customer'
	 end as customer_segment
	 from cte2

--Year wise revenue.
select
extract(year from invoice_date) as years,
sum(revenue) as total_revenue
from gold.fact_sales
group by years

--Month wise revenue per year.
select
extract(year from invoice_date) as years,
extract(month from invoice_date) as months,
sum(revenue) as total_revenue
from gold.fact_sales
group by years, months

--YoY Growth
with cte as(
select
extract(year from invoice_date) as years,
sum(revenue) as current_year_revenue
from gold.fact_sales
group by years
)
,cte2 as(
select
*,
lag(current_year_revenue,1) over(order by years) as previous_year_revenue
from cte
)
select 
*,
case when previous_year_revenue is null or previous_year_revenue = 0
     then null
	 else concat(round((current_year_revenue - previous_year_revenue)*100 / previous_year_revenue, 2),'%')
	 end as YoY_growth
from cte2

--MoM growth per year.
with cte as(
select
extract(year from invoice_date) as years,
extract(month from invoice_date) as months,
sum(revenue) as current_month_revenue
from gold.fact_sales
group by years, months
)
,cte2 as(
select
*,
lag(current_month_revenue) over(partition by years order by months) as previous_month_revenue
from cte
)
select
*,
case when previous_month_revenue is null or previous_month_revenue = 0
     then null
	 else concat(round((current_month_revenue - previous_month_revenue)*100 / previous_month_revenue,2),'%')
	 end as MoM_growth
from cte2

--Top 5 countries with most revenue
with cte as(
select
c.country,
sum(s.revenue) as total_revenue
from gold.fact_sales s
left join gold.dim_customer c
on c.customer_id = s.customer_id
group by country
)
,cte2 as (
select
*,
dense_rank() over(order by total_revenue desc) as ranking
from cte
)
select
*
from cte2
where ranking <= 5

