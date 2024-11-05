use Amazon 
/*
LOAD DATA  INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cc_add.csv'
INTO TABLE  cc_detail
FIELDS TERMINATED  BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
ignore 1 rows;
*/

create database Amazon;

select count(*) from amazon_sales_df1;

select * from amazon_sales_df;

select distinct (`ship-city`) from amazon_sales_df;


drop table amazon_sales_df if exists;



CREATE TABLE `amazon_sales_df` (
  `order_id` text,
  `date` datetime DEFAULT NULL,
  `status` text,
  `fulfilment` text,
  `sales_channel` text,
  `ship_service_level` text,
  `category` text,
  `size` text,
  `courier_status` text,
  `qty` int DEFAULT NULL,
  `currency` text,
  `Amount` double DEFAULT NULL,
  `ship_city` text,
  `ship_state` text,
  `ship_postal_code` int DEFAULT NULL,
  `ship_country` text,
  `b2b` text,
  `fulfilled_by` text
)

select * from amazon_sales_df where amount is null;

create table amazon_sales_df2 like amazon_sales_df1
insert into amazon_sales_df2
select * from amazon_sales_df1

-- check for duplicates
select * from 
(select *,
rank() over(partition by date,status,fulfilment,sales_channel,ship_service_level,category,size,qty,amount,ship_city,ship_state,ship_postal_code) as rn
from amazon_sales_df1) A where rn>1

-- handling misising values

select count(*) from amazon_sales_df2


select * from amazon_sales_df2 where currency is null or ship_city is null or ship_state is null ;

select count(*) from amazon_sales_df2 where ship_country is null and ship_country='IN';

update amazon_sales_df2 set currency='INR' where currency is null and ship_country='IN';

update amazon_sales_df2 set ship_city='unknown',ship_postal_code=0,ship_country='unknown'  where  ship_country is null and 
ship_city is null and ship_postal_code is null and ship_country is null;

update amazon_sales_df2 set ship_state='unknown'  where  ship_country = 'unknown';

update amazon_sales_df2 set currency='unknown'  where  ship_country = 'unknown' and currency is null;

update amazon_sales_df2 set amount=0 where amount is null and ship_country='IN' and currency='INR';

select * from amazon_sales_df2 where amount is null

-- Analysis

-- show variables tns_admin

select fulfilment,`status`,count(*) from amazon_sales_df2 group by fulfilment,`status` order by 3 desc

select fulfilment,status, count(*) total_orders,
rank() over (partition by fulfilment order by count(*) desc ) as rn
from amazon_sales_df2 group by fulfilment,status 
-- 11475 cancelled Amazon 
-- 6859 cancelled merchant













