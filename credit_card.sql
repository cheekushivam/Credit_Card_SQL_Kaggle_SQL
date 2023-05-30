
CREATE TABLE Credit_Card_Transactions
(
   CITY VARCHAR,
   Date Date,
   CARD_TYPE VARCHAR,
   EXP_TYPE VARCHAR,
   GENDER VARCHAR,
   AMOUNT INT
);

-- Load Data (CSV File) From local file system to Credit_Card_Transactions Table
Copy Credit_Card_Transactions(City,Date,Card_Type,Exp_Type,Gender,Amount)
FROM 'D:\Personal_Documents\Credit_Card_Transactions_Dataa.csv'
Delimiter ','
CSV HEADER;

-- Basic Check Of Data
Select count(*) from Credit_Card_Transactions;
Select * from Credit_Card_Transactions;


--1. write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends 

with Total_Spend_Per_City As (
Select 
	x.city, 
	x.total_spend 
	from ( select city, sum(amount) as total_spend from credit_card_transactions group by city ) as x
	order by x.total_spend desc limit 5
)
,Total_Spend_Overall As (
Select 
	sum(amount) as total_spend_all 
	from credit_card_transactions
)
--select * from Total_Spend_Per_City,Total_Spend_Overall;
Select 
	x.city, 
	x.total_spend, 
	ROUND(cast(x.total_spend as decimal)/x.total_spend_all * 100,2)  as contribution
From (select * from Total_Spend_Per_City,Total_Spend_Overall) as x;

--Result
-- "city"	"total_spend"	"contribution"
-- "Greater Mumbai, India"	576751476	14.15
-- "Bengaluru, India"	572326739	14.05
-- "Ahmedabad, India"	567794310	13.93
-- "Delhi, India"	556929212	13.67
-- "Kolkata, India"	115466943	2.83


--2. write a query to print highest spend month and amount spent in that month for each card type
with map_month_year AS (
	select *, 
		Extract(month from date) as spend_month, 
		Extract(year from date) as spend_year  
	from credit_card_transactions
)
,highest_spend_month AS (
	select 
		spend_month, 
		spend_year, 
		sum(amount) as spend 
	from map_month_year group by spend_month, spend_year order by spend desc limit 1
)
select 
	mmy.spend_month, mmy.spend_year, mmy.card_type, sum(mmy.amount)
from highest_spend_month as hsm join map_month_year as mmy 
on hsm.spend_month = mmy.spend_month and hsm.spend_year = mmy.spend_year
Group by mmy.card_type, mmy.spend_month, mmy.spend_year;

--Result
"spend_month"	"spend_year"	"card_type"	"sum"
1	2015	"Gold"	55455064
1	2015	"Platinum"	57850182
1	2015	"Signature"	52774683
1	2015	"Silver"	57478645

--3. write a query to print the transaction details(all columns from the table) for each card type when
--it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)


with apply_rolling_sum AS (
 select *, sum(amount) over(partition by card_type order by date) as sum_till_date from credit_card_transactions
)
,apply_dense_rank AS (
	select *, dense_rank()Over(partition by card_type order by sum_till_date) rank_amount from apply_rolling_sum  as a where a.sum_till_date >= 1000000
)
select card_type, date from apply_dense_rank where rank_amount = 1 group by card_type, date;

--result
-- "card_type"	"date"
-- "Gold"	"2013-10-04"
-- "Platinum"	"2013-10-05"
-- "Signature"	"2013-10-04"
-- "Silver"	"2013-10-04"


--4. write a query to find city which had lowest percentage spend for gold card type

with Total_Spend_Overall_Gold As (
Select 
	city, sum(amount) as total_spend_all 
	from credit_card_transactions
	group by city
)
,Lowest_Spend_city_for_gold AS (
select 
	city, card_type, sum(amount) as amount 
	from credit_card_transactions where card_type = 'Gold'
	group by city, card_type
	order by amount limit 1
)
select x.city, x.card_type, 
x.amount, ROUND(cast(x.amount as decimal)/x.total_spend_all * 100,2)  as pct_contribution
From ( 
	select l.*, t.total_spend_all 
	from Lowest_Spend_city_for_gold as l inner join Total_Spend_Overall_Gold as t 
	on l.city = t.city
) as x;

--Result
-- "city"	"card_type"	"amount"	"pct_contribution"
-- "Dhamtari, India"	"Gold"	1416	0.33

--5. write a query to print 3 columns: city, highest_expense_type , lowest_expense_type 
--(example format : Delhi , bills, Fuel)

with total_spend_city_exp_type AS (
select city, exp_type, sum(amount) as amount_spend
	from credit_card_transactions
	group by city, exp_type
)
,find_highest_expense_type AS (
select B.city, A.exp_type as highest_expense_type from total_spend_city_exp_type as A inner join 
	( select city, max(amount_spend) as max_spend from total_spend_city_exp_type group by city ) as B
		on A.city = B.city and A.amount_spend = B.max_spend
)
,find_lowest_expense_type AS (
select B.city, A.exp_type as lowest_expense_type from total_spend_city_exp_type as A inner join 
	( select city, min(amount_spend) as min_spend from total_spend_city_exp_type group by city ) as B
		on A.city = B.city and A.amount_spend = B.min_spend
)
Select h.city, h.highest_expense_type, l.lowest_expense_type 
from find_highest_expense_type as h inner join find_lowest_expense_type as l 
on h.city = l.city
order by h.city;

--6. write a query to find percentage contribution of spends by females for each expense type

with total_spend_gender_exp_type AS (
select exp_type, sum(amount) as amount_spend_by_female
	from credit_card_transactions
	where gender = 'F'
	group by exp_type
)
, total_spend_exp_type AS (
	select exp_type, sum(amount) as total_amount_spend
	from credit_card_transactions
	group by exp_type
)
Select 
x.exp_type as expense_type,
x.amount_spend_by_female,
x.total_amount_spend,
ROUND(cast(x.amount_spend_by_female as decimal)/x.total_amount_spend * 100,2)  as pct_contribution
From ( select a.*, b.total_amount_spend 
	  from total_spend_gender_exp_type as a inner join total_spend_exp_type as b
	  on a.exp_type = b.exp_type ) as x
	  order by x.exp_type
	  
--Result
-- "expense_type"	"amount_spend_by_female"	"total_amount_spend"	"pct_contribution"
-- "Bills"	580035469	907072473	63.95
-- "Entertainment"	358663333	726437536	49.37
-- "Food"	452817279	824724009	54.91
-- "Fuel"	392282421	789135821	49.71
-- "Grocery"	365646998	718207923	50.91
-- "Travel"	55865530	109255611	51.13

--7.which card and expense type combination saw highest month over month growth in Jan-2014

with month_year_spend as (
	select 
	card_type, 
	exp_type, 
	Extract(month from date) as spend_month, 
	Extract(year from date) as spend_year,
	sum(amount) as spend
	from credit_card_transactions
	Group By card_type, exp_type, spend_month, spend_year
)	
,get_prev_spend as (
	select 
	*
	,lag(spend,1)over(partition by card_type, exp_type order by spend_year, spend_month) as lag_spend
	from month_year_spend
)	
select *, 
(spend-lag_spend) as growth
from get_prev_spend
where spend_month = 1 and spend_year = 2014 and(spend-lag_spend) > 0 
order by (spend-lag_spend) desc limit 1;


-- "card_type"	"exp_type"	"spend_month"	"spend_year"	"spend"	"lag_spend"	"growth"
-- "Platinum"	"Grocery"	1	2014	12256343	7757562	4498781

--8. during weekends which city has highest total spend to total no of transcations ratio 

with weekend_identifier AS (
	select 
	*,
	case when EXTRACT(dow FROM date) IN (0,6) THEN 'weekend' else 'weekday' end as wflag
	from credit_card_transactions
)
,sum_weekend_per_city as (
	select city, sum(amount) as amt from weekend_identifier where wflag = 'weekend' group by city
)
,total_transactions_per_city AS (
	select city, count(*) as cnt from weekend_identifier where wflag = 'weekend' group by city 
)
select x.city, x.amt, x.cnt as txns, Round((cast(x.amt as decimal)/x.cnt),2) as ratio from  
(select ww.city, tt.cnt, ww.amt from total_transactions_per_city as tt join sum_weekend_per_city as ww on tt.city = ww.city) x
order by ratio desc limit 1

--result
-- "city"	"amt"	"txns"	"ratio"
-- "Sonepur, India"	299905	1	299905.00

--9. which city took least number of days to reach its 500th transaction after first transaction in that city
with get_first_day_txn_dates  AS (
	select x.city, x.date from  
		( select city, date, dense_rank()over(partition by city order by date asc) rank_txns from credit_card_transactions) x
	where x.rank_txns = 1
)
, get_500th_day_txn_dates AS (
	select x.city, x.date, x.rns from
		( select city, date, ROW_NUMBER()over(partition by city order by date asc) rns from credit_card_transactions) x
	where x.rns = 500
)
select f.city as city, f.date as first_txn_date, l.date as txn_date_500th, l.date-f.date as days
from get_first_day_txn_dates as f join get_500th_day_txn_dates as l on f.city = l.city
order by l.date-f.date limit 1;

--result
-- "city"	"first_txn_date"	"txn_date_500th"	"days"
-- "Bengaluru, India"	"2013-10-04"	"2013-12-24"	81






































