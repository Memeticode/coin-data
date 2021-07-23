
-- pth order autoregression

-- autoregressive distributed lag model

-- forecast next change in price as a result of:
	-- price
	-- trading volume
	-- interval price variance
	-- other price
	-- other trading volume
	-- other interval price variance

/*
	forecasted price = 
		constant
		+ B1 (price t-1)
		+ B2 (volume t-1)
		+ B3 (variance t-1)

		+ unobserved term

*/


use CoinData;
go

-- Try using logs for better results
create table dbo.ProductHistoricRatesUSD_RegressionInputs (
	product_fk int not null foreign key references dbo.ProductUSD(id),
	granularity_fk int not null foreign key references dbo.RateGranularity(id),
	[start_datetime] datetime2 not null,
	[timestamp] nvarchar(10) not null,
	[interval_price_variance] decimal(26,8) not null,
	[est_avg_price] decimal(26,8) not null,
	[est_avg_trade_volume] decimal(26,8) not null,
	[pct_change_interval_price_variance] float null,
	[pct_change_est_avg_price] float null,
	[pct_change_est_avg_trade_volume] float null,
	unique(product_fk, granularity_fk, [timestamp])
);
go

-- 33,568,060

select 
	(select count(*) from dbo.ProductHistoricRatesUSD) as orig_metrics,
	(select count(*) from dbo.ProductHistoricRatesUSD_RegressionInputs) as reg_metrics
go

With PriceMetrics as (
	select 
		a.product_fk, a.granularity_fk, a.start_datetime, a.[timestamp]
		
		, interval_price_variance = a.[high] - a.[low]

		, est_avg_price = 
			dbo.fnEstimatedAvgPricePerInterval(a.[low], a.[high], a.[open], a.[close]) 
		
		, est_avg_trade_volume = 
			dbo.fnPriceVolumePerInterval(
				dbo.fnEstimatedAvgPricePerInterval(a.[low], a.[high], a.[open], a.[close]), 
				a.volume
			)
	from CoinData.dbo.ProductHistoricRatesUSD a
)
, PrevPriceMetrics as (
	select a.*
		, prev_interval_price_variance	= lag(a.interval_price_variance) over(partition by a.product_fk, a.granularity_fk order by a.[timestamp] asc)
		, prev_est_avg_price			= lag(a.est_avg_price) over(partition by a.product_fk, a.granularity_fk order by a.[timestamp] asc)
		, prev_est_avg_trade_volume		= lag(a.est_avg_trade_volume) over(partition by a.product_fk, a.granularity_fk order by a.[timestamp] asc)
	from PriceMetrics a
) 
insert into dbo.ProductHistoricRatesUSD_RegressionInputs
	select 
		a.product_fk, a.granularity_fk, a.start_datetime, a.[timestamp]
		, a.interval_price_variance
		, a.est_avg_price
		, a.est_avg_trade_volume 
		, pct_change_interval_price_variance	= dbo.fnPctChangePrice(a.prev_interval_price_variance, a.interval_price_variance)
		, pct_change_est_avg_price				= dbo.fnPctChangePrice(a.prev_est_avg_price, a.est_avg_price)
		, pct_change_est_avg_trade_volume		= dbo.fnPctChangePrice(a.prev_est_avg_trade_volume, a.est_avg_trade_volume)
	from PrevPriceMetrics a
go



