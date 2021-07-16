
/* 

use CoinData;
go

create function dbo.fnEstimatedAvgPricePerInterval(@low decimal(26,8), @high decimal(26,8), @open decimal(26,8), @close decimal(26,8))
returns decimal(26,8)
begin
	-- calc could include close price, but let's estimate avg by considering high and low instead
	-- seems to make more sense
	return (@low + @high + @open + @close) / 4
end
go


create function dbo.fnPriceVolumePerInterval(@price decimal(26,8), @volume decimal(26,8))
returns decimal(26,8)
begin
	return (@price * @volume)
end
go

create function dbo.fnPctChangePrice(@price1 decimal(26,8), @price2 decimal(26,8))
returns decimal(26,8)
begin
	return (@price2 - @price1) / @price1
end
go

*/


/*
	-- Calculate metrics used for base regressions (cross-coin)

*/
select * from dbo.ModelParamTimespanIntervals
select count(*) from dbo.ProductUSD
select max(start_datetime) from dbo.ProductHistoricRatesUSD where granularity_fk = 6

select count(*), count(distinct product_fk)  
	from CoinData.dbo.ProductHistoricRatesUSD a
	where 1=1
	and granularity_fk = 6
--	and a.start_datetime between '2021-05-01' and '2021-06-01'
	and cast(a.start_datetime as date) = '2021-06-28' 


-- Regressions for product against all other products for specified timespan
declare 
@reg_interval_span int		= 8
, @granularity nvarchar(10)	= '1day'
, @product nvarchar(5)		= 'FORTH'
, @granularity_fk int
, @product_fk int;

select @granularity_fk = id from CoinData.dbo.RateGranularity where [desc] = @granularity;
select @product_fk = id from CoinData.dbo.ProductUSD where product = @product;
if (@granularity_fk is null) begin; throw 5100, 'Specified granularity does not exist', 1; end;
if (@product_fk is null) begin; throw 5100, 'Specified product does not exist', 1; end;


declare 
@comp_granularity nvarchar(10)	= '1day'
, @comp_product nvarchar(5)		= 'BTC'
, @comp_granularity_fk int
, @comp_product_fk int;

select @comp_granularity_fk = id from CoinData.dbo.RateGranularity where [desc] = @comp_granularity;
select @comp_product_fk = id from CoinData.dbo.ProductUSD where product = @comp_product;
if (@comp_granularity_fk is null) begin; throw 5100, 'Specified granularity does not exist', 1; end;
if (@comp_product_fk is null) begin; throw 5100, 'Specified product does not exist', 1; end;


With EstAvgPrice as (
	select 
		a.product_fk, a.granularity_fk, a.start_datetime, a.[timestamp]
		, a.[low], a.[high], a.[open], a.[close], a.volume
		
		, est_avg_price = 
			dbo.fnEstimatedAvgPricePerInterval(a.[low], a.[high], a.[open], a.[close]) 
		
		, est_avg_price_volume = 
			dbo.fnPriceVolumePerInterval(
				dbo.fnEstimatedAvgPricePerInterval(a.[low], a.[high], a.[open], a.[close]), 
				a.volume
			)
	
	from CoinData.dbo.ProductHistoricRatesUSD a
	where 1=1
	and a.granularity_fk = @granularity_fk
)
, LogMetrics as (
	select a.*
		, log(a.est_avg_price) as log_est_avg_price
		, log(a.est_avg_price_volume) as log_est_avg_price_volume
	from EstAvgPrice a
)
--select count(*), count(distinct y_product_fk) from (
select a.product_fk
	, a.granularity_fk
	, a.start_datetime
	, a.[timestamp]
	, a.log_est_avg_price
	, a.log_est_avg_price_volume
	, b.product_fk					as y_product_fk
	, b.log_est_avg_price			as y_log_est_avg_price
	, b.log_est_avg_price_volume	as y_log_est_avg_price_volume
from LogMetrics a
left join LogMetrics b on a.[timestamp] = b.[timestamp]
						and a.granularity_fk = b.granularity_fk
						and a.product_fk <> b.product_fk
where a.product_fk = @product_fk
--order by b.product_fk, a.start_datetime


