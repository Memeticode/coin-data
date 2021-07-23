-- from this, should only trade / include products w/ at least 234 days worth of data... eh

use CoinData;
go

-- Moving average of est_avg_price from 
-- dbo.ProductHistoricRatesUSD_RegressionInputs

select * from dbo.ModelParamTimespanIntervals
-- Intervals:
	-- 8, 13, 21, 34, 55, 89, 144, 233

create table dbo.ProductHistoricRatesUSD_MovingAvgs (
	product_fk int not null foreign key references dbo.ProductUSD(id),
	granularity_fk int not null foreign key references dbo.RateGranularity(id),
	[start_datetime] datetime2 not null,
	[timestamp] nvarchar(10) not null,
	[est_avg_price] decimal(26,8) not null,
	[sma8] decimal(26,8) null,		[sma8_pct_diff] float null,		
	[sma13] decimal(26,8) null,		[sma13_pct_diff] float null,
	[sma21] decimal(26,8) null,		[sma21_pct_diff] float null,
	[sma34] decimal(26,8) null,		[sma34_pct_diff] float null,
	[sma55] decimal(26,8) null,		[sma55_pct_diff] float null,
	[sma89] decimal(26,8) null,		[sma89_pct_diff] float null,
	[sma144] decimal(26,8) null,	[sma144_pct_diff] float null,
	[sma233] decimal(26,8) null,	[sma233_pct_diff] float null,	
	unique(product_fk, granularity_fk, [timestamp])
);
go


select 
	(select count(*) from dbo.ProductHistoricRatesUSD_RegressionInputs) as orig_metrics,
	(select count(*) from dbo.ProductHistoricRatesUSD_MovingAvgs) as reg_metrics
go

with sma as (
	select a.product_fk, a.granularity_fk, a.start_datetime, a.[timestamp]
		, a.est_avg_price
		, sma8		= avg(a.est_avg_price) over (partition by a.product_fk, a.granularity_fk order by a.[timestamp] rows between 7		preceding and current row)
		, sma13		= avg(a.est_avg_price) over (partition by a.product_fk, a.granularity_fk order by a.[timestamp] rows between 12		preceding and current row)
		, sma21		= avg(a.est_avg_price) over (partition by a.product_fk, a.granularity_fk order by a.[timestamp] rows between 20		preceding and current row)
		, sma34		= avg(a.est_avg_price) over (partition by a.product_fk, a.granularity_fk order by a.[timestamp] rows between 33		preceding and current row)
		, sma55		= avg(a.est_avg_price) over (partition by a.product_fk, a.granularity_fk order by a.[timestamp] rows between 54		preceding and current row)
		, sma89		= avg(a.est_avg_price) over (partition by a.product_fk, a.granularity_fk order by a.[timestamp] rows between 88		preceding and current row)
		, sma144	= avg(a.est_avg_price) over (partition by a.product_fk, a.granularity_fk order by a.[timestamp] rows between 143	preceding and current row)
		, sma233	= avg(a.est_avg_price) over (partition by a.product_fk, a.granularity_fk order by a.[timestamp] rows between 232	preceding and current row)
	from dbo.ProductHistoricRatesUSD_RegressionInputs a
)
, sma_diff as (
	select a.*
		, sma8_pct_diff		= dbo.fnPctChangePrice(a.sma8	, a.est_avg_price)
		, sma13_pct_diff	= dbo.fnPctChangePrice(a.sma13	, a.est_avg_price)
		, sma21_pct_diff	= dbo.fnPctChangePrice(a.sma21	, a.est_avg_price)
		, sma34_pct_diff	= dbo.fnPctChangePrice(a.sma34	, a.est_avg_price)
		, sma55_pct_diff	= dbo.fnPctChangePrice(a.sma55	, a.est_avg_price)
		, sma89_pct_diff	= dbo.fnPctChangePrice(a.sma89	, a.est_avg_price)
		, sma144_pct_diff	= dbo.fnPctChangePrice(a.sma144	, a.est_avg_price)
		, sma233_pct_diff	= dbo.fnPctChangePrice(a.sma233	, a.est_avg_price)
	from sma a
)
insert into dbo.ProductHistoricRatesUSD_MovingAvgs
select
	a.product_fk, a.granularity_fk, a.start_datetime, a.[timestamp]
	, a.est_avg_price
	, a.[sma8]	
	, a.[sma8_pct_diff]
	, a.[sma13]	
	, a.[sma13_pct_diff]
	, a.[sma21]	
	, a.[sma21_pct_diff]
	, a.[sma34]	
	, a.[sma34_pct_diff]
	, a.[sma55]	
	, a.[sma55_pct_diff]
	, a.[sma89]	
	, a.[sma89_pct_diff]
	, a.[sma144]	
	, a.[sma144_pct_diff]
	, a.[sma233]	
	, a.[sma233_pct_diff]
from sma_diff a
order by a.product_fk, a.granularity_fk, a.[timestamp]
go


select 
	(select count(*) from dbo.ProductHistoricRatesUSD_RegressionInputs) as orig_metrics,
	(select count(*) from dbo.ProductHistoricRatesUSD_MovingAvgs) as reg_metrics
go

