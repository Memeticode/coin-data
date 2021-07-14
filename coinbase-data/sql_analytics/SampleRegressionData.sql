
use CoinData

-- Regressions for product against all other products for specified timespan
declare 
@reg_interval_span int		= 50
, @granularity nvarchar(10)	= '1day'
, @product nvarchar(5)		= 'REN'
, @comp_product nvarchar(5)	= 'BTC'
, @granularity_fk int
, @granularity_sec int
, @product_fk int
, @comp_product_fk int;

select @product_fk = id from CoinData.dbo.ProductUSD where product = @product;
if (@product_fk is null) begin; throw 5100, 'Specified product does not exist', 1; end;

select @granularity_fk = id, @granularity_sec = seconds from CoinData.dbo.RateGranularity where [desc] = @granularity;
if (@granularity_fk is null or @granularity_sec is null) begin; throw 5100, 'Specified granularity does not exist', 1; end;

select @comp_product_fk = id from CoinData.dbo.ProductUSD where product = @comp_product;
if (@comp_product_fk is null) begin; throw 5100, 'Specified product does not exist', 1; end;


With Product as (
	select top(50) a.*
	from CoinData.dbo.ProductHistoricRatesUSD_RegressionInputs a
	where 1=1
	and a.granularity_fk = @granularity_fk
	and a.product_fk = @product_fk
	order by a.timestamp desc
)
select a.* 
	, b.interval_price_variance				as comp_interval_price_variance
	, b.est_avg_price						as comp_est_avg_price
	, b.est_avg_trade_volume				as comp_est_avg_trade_volume
	, b.pct_change_interval_price_variance	as comp_pct_change_interval_price_variance
	, b.pct_change_est_avg_price			as comp_pct_change_est_avg_price
	, b.pct_change_est_avg_trade_volume		as comp_pct_change_est_avg_trade_volume
into dbo.SampleRegressionData
from Product a
left join CoinData.dbo.ProductHistoricRatesUSD_RegressionInputs b 
	on a.[timestamp] = b.[timestamp]
	and a.granularity_fk = b.granularity_fk
where 1=1
and b.product_fk = @comp_product_fk
;

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



select row_number() over(order by timestamp), * from dbo.SampleRegressionData