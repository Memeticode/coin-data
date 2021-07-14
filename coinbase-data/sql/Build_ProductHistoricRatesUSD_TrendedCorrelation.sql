use CoinData;

go

--DROP table dbo.ProductHistoricRatesUSD_TrendedCorrelation

create table dbo.ProductHistoricRatesUSD_TrendedCorrelation (
	 product_fk						int not null foreign key references dbo.ProductUSD(id)
	,x_product_fk					int not null foreign key references dbo.ProductUSD(id)
	,granularity_fk					int not null foreign key references dbo.RateGranularity(id)
	,start_datetime					datetime2 not null
	,[timestamp]					nvarchar(10) not null
	,pct_change_est_avg_price		float null
	,x_pct_change_est_avg_price		float null
	,reg3_n							float null
	,reg3_x_bar						float null
	,reg3_x_stdev					float null
	,reg3_y_bar						float null
	,reg3_y_stdev					float null
	,reg3_b							float null
	,reg3_r							float null
	,reg3_rsq						float null
	,reg3_resid						float null
	,reg8_n							float null
	,reg8_x_bar						float null
	,reg8_x_stdev					float null
	,reg8_y_bar						float null
	,reg8_y_stdev					float null
	,reg8_b							float null
	,reg8_r							float null
	,reg8_rsq						float null
	,reg8_resid						float null
	,reg21_n						float null
	,reg21_x_bar					float null
	,reg21_x_stdev					float null
	,reg21_y_bar					float null
	,reg21_y_stdev					float null
	,reg21_b						float null
	,reg21_r						float null
	,reg21_rsq						float null
	,reg21_resid					float null
	,unique(product_fk, x_product_fk, granularity_fk, [timestamp])
);
go

select count(*) cnt
, count(distinct product_fk) as dist_product_fk
, count(distinct x_product_fk) as dist_x_product_fk
, count(distinct granularity_fk) as dist_granularity_fk
from dbo.ProductHistoricRatesUSD_TrendedCorrelation
;
go

With RegMetrics1 as (
	select
		a.product_fk
		,a.granularity_fk
		,a.start_datetime
		,a.[timestamp]
		,a.pct_change_est_avg_price 
		,b.pct_change_est_avg_price	as x_pct_change_est_avg_price
		,b.product_fk				as x_product_fk
		,b.granularity_fk			as x_granularity_fk
	-- 3 period regression
		, reg3_n			= sum(1.0)														over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_y_sum		= sum(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_y_bar		= avg(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_ysq_sum		= sum(square(a.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_y_stdev		= stdev(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_y_var		= var(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_x_sum		= sum(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_x_bar		= avg(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_xsq_sum		= sum(square(b.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_x_stdev		= stdev(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_x_var		= var( b.pct_change_est_avg_price )								over (order by a.[timestamp] rows between 2 preceding and current row)
		, reg3_xy_sum		= sum(a.pct_change_est_avg_price * b.pct_change_est_avg_price)	over (order by a.[timestamp] rows between 2 preceding and current row)
	-- 8 period regression
		, reg8_n			= sum(1.0)														over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_y_sum		= sum(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_y_bar		= avg(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_ysq_sum		= sum(square(a.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_y_stdev		= stdev(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_y_var		= var(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_x_sum		= sum(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_x_bar		= avg(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_xsq_sum		= sum(square(b.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_x_stdev		= stdev(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_x_var		= var( b.pct_change_est_avg_price )								over (order by a.[timestamp] rows between 7 preceding and current row)
		, reg8_xy_sum		= sum(a.pct_change_est_avg_price * b.pct_change_est_avg_price)	over (order by a.[timestamp] rows between 7 preceding and current row)
	-- 21 period regression
		, reg21_n			= sum(1.0)														over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_y_sum		= sum(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_y_bar		= avg(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_ysq_sum		= sum(square(a.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_y_stdev		= stdev(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_y_var		= var(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_x_sum		= sum(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_x_bar		= avg(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_xsq_sum		= sum(square(b.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_x_stdev		= stdev(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_x_var		= var( b.pct_change_est_avg_price )								over (order by a.[timestamp] rows between 20 preceding and current row)
		, reg21_xy_sum		= sum(a.pct_change_est_avg_price * b.pct_change_est_avg_price)	over (order by a.[timestamp] rows between 20 preceding and current row)

	from dbo.ProductHistoricRatesUSD_RegressionInputs a
	join dbo.ProductHistoricRatesUSD_RegressionInputs b
		on a.product_fk <> b.product_fk
		and a.granularity_fk = b.granularity_fk
		and a.[timestamp] = b.[timestamp]
	where 1=1
	and a.pct_change_est_avg_price is not null
	and b.pct_change_est_avg_price is not null
)
, RegMetrics2 as (
	select a.*
		,reg3_r		=  case when ( (a.reg3_n * a.reg3_xsq_sum) - square(a.reg3_x_sum) ) * ( (a.reg3_n * a.reg3_ysq_sum) - square(a.reg3_y_sum) ) <= 0 then null
							else ( (a.reg3_n * a.reg3_xy_sum) - (a.reg3_x_sum * a.reg3_y_sum) ) / SQRT( ( (a.reg3_n * a.reg3_xsq_sum) - square(a.reg3_x_sum) ) * ( (a.reg3_n * a.reg3_ysq_sum) - square(a.reg3_y_sum) ) ) end
		,reg8_r		=  case when ( (a.reg8_n * a.reg8_xsq_sum) - square(a.reg8_x_sum) ) * ( (a.reg8_n * a.reg8_ysq_sum) - square(a.reg8_y_sum) ) <= 0 then null
							else ( (a.reg8_n * a.reg8_xy_sum) - (a.reg8_x_sum * a.reg8_y_sum) ) / SQRT( ( (a.reg8_n * a.reg8_xsq_sum) - square(a.reg8_x_sum) ) * ( (a.reg8_n * a.reg8_ysq_sum) - square(a.reg8_y_sum) ) ) end
		,reg21_r		=  case when ( (a.reg21_n * a.reg21_xsq_sum) - square(a.reg21_x_sum) ) * ( (a.reg21_n * a.reg21_ysq_sum) - square(a.reg21_y_sum) ) <= 0 then null
							else ( (a.reg21_n * a.reg21_xy_sum) - (a.reg21_x_sum * a.reg21_y_sum) ) / SQRT( ( (a.reg21_n * a.reg21_xsq_sum) - square(a.reg21_x_sum) ) * ( (a.reg21_n * a.reg21_ysq_sum) - square(a.reg21_y_sum) ) ) end
	from RegMetrics1 a
)
, RegMetrics3 as (
	select a.*
		, reg3_rsq	= square( a.reg3_r )
		, reg3_cov	= a.reg3_r * a.reg3_x_stdev * a.reg3_y_stdev
		, reg8_rsq	= square( a.reg8_r )
		, reg8_cov	= a.reg8_r * a.reg8_x_stdev * a.reg8_y_stdev
		, reg21_rsq	= square( a.reg21_r )
		, reg21_cov	= a.reg21_r * a.reg21_x_stdev * a.reg21_y_stdev
	from RegMetrics2 a
)
, RegMetrics4 as (
	select a.*
		, reg3_b	= a.reg3_cov / nullif(a.reg3_x_var, 0)
		, reg8_b	= a.reg8_cov / nullif(a.reg8_x_var, 0)
		, reg21_b	= a.reg21_cov / nullif(a.reg21_x_var, 0)
	from RegMetrics3 a
)
, RegMetrics5 as (
	select a.*
		, reg3_a	= a.reg3_y_bar - ( a.reg3_x_bar * a.reg3_b )
		, reg8_a	= a.reg8_y_bar - ( a.reg8_x_bar * a.reg8_b )
		, reg21_a	= a.reg21_y_bar - ( a.reg21_x_bar * a.reg21_b )
	from RegMetrics4 a
)
, RegMetrics6 as (
	select a.*
		, reg3_pred		= ( a.reg3_b * a.x_pct_change_est_avg_price ) + a.reg3_a
		, reg3_resid	= a.pct_change_est_avg_price - ( ( a.reg3_b * a.x_pct_change_est_avg_price ) + a.reg3_a )
		, reg8_pred		= ( a.reg8_b * a.x_pct_change_est_avg_price ) + a.reg8_a
		, reg8_resid	= a.pct_change_est_avg_price - ( ( a.reg8_b * a.x_pct_change_est_avg_price ) + a.reg8_a )
		, reg21_pred	= ( a.reg21_b * a.x_pct_change_est_avg_price ) + a.reg21_a
		, reg21_resid	= a.pct_change_est_avg_price - ( ( a.reg21_b * a.x_pct_change_est_avg_price ) + a.reg21_a )
	from RegMetrics5 a
)
insert into dbo.ProductHistoricRatesUSD_TrendedCorrelation
select 
	a.product_fk
	,a.x_product_fk
	,a.granularity_fk
	,a.start_datetime
	,a.[timestamp]
	,a.pct_change_est_avg_price
	,a.x_pct_change_est_avg_price

	,a.reg3_n
	,a.reg3_x_bar
	,a.reg3_x_stdev
	,a.reg3_y_bar
	,a.reg3_y_stdev
	,a.reg3_b
	,a.reg3_r
	,a.reg3_rsq
	,a.reg3_resid

	,a.reg8_n
	,a.reg8_x_bar
	,a.reg8_x_stdev
	,a.reg8_y_bar
	,a.reg8_y_stdev
	,a.reg8_b
	,a.reg8_r
	,a.reg8_rsq
	,a.reg8_resid
	
	,a.reg21_n
	,a.reg21_x_bar
	,a.reg21_x_stdev
	,a.reg21_y_bar
	,a.reg21_y_stdev
	,a.reg21_b
	,a.reg21_r
	,a.reg21_rsq
	,a.reg21_resid

from RegMetrics6 a
order by a.[timestamp] desc
-- rsq

go

select count(*) cnt
, count(distinct product_fk) as dist_product_fk
, count(distinct x_product_fk) as dist_x_product_fk
, count(distinct granularity_fk) as dist_granularity_fk
from dbo.ProductHistoricRatesUSD_TrendedCorrelation
;
go