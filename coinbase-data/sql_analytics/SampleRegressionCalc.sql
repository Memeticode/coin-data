


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
		, reg3_n			= sum(1.0)														over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_y_sum		= sum(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_y_bar		= avg(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_ysq_sum		= sum(square(a.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_y_stdev		= stdev(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_y_var		= var(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_x_sum		= sum(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_x_bar		= avg(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_xsq_sum		= sum(square(b.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_x_stdev		= stdev(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_x_var		= var( b.pct_change_est_avg_price )								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg3_xy_sum		= sum(a.pct_change_est_avg_price * b.pct_change_est_avg_price)	over (order by a.[timestamp] rows between 265 preceding and current row)

	-- 8 period regression
		, reg8_n			= sum(1.0)														over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_y_sum		= sum(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_y_bar		= avg(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_ysq_sum		= sum(square(a.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_y_stdev		= stdev(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_y_var		= var(a.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_x_sum		= sum(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_x_bar		= avg(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_xsq_sum		= sum(square(b.pct_change_est_avg_price))						over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_x_stdev		= stdev(b.pct_change_est_avg_price)								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_x_var		= var( b.pct_change_est_avg_price )								over (order by a.[timestamp] rows between 265 preceding and current row)
		, reg8_xy_sum		= sum(a.pct_change_est_avg_price * b.pct_change_est_avg_price)	over (order by a.[timestamp] rows between 265 preceding and current row)

	from dbo.ProductHistoricRatesUSD_RegressionInputs a
	join dbo.ProductHistoricRatesUSD_RegressionInputs b
		on a.product_fk <> b.product_fk
		and a.granularity_fk = b.granularity_fk
		and a.[timestamp] = b.[timestamp]
	where 1=1
	and a.pct_change_est_avg_price is not null
	and b.pct_change_est_avg_price is not null
	and a.granularity_fk = 6 -- 1 day
	and a.product_fk = 51 -- ren
	and b.product_fk = 14 -- btc
	--order by a.[timestamp] desc
)
, RegMetrics2 as (
	select a.*
		,reg3_r		= ( (a.reg3_n * a.reg3_xy_sum) - (a.reg3_x_sum * a.reg3_y_sum) ) / NULLIF( SQRT( ( (a.reg3_n * a.reg3_xsq_sum) - square(a.reg3_x_sum) ) * ( (a.reg3_n * a.reg3_ysq_sum) - square(a.reg3_y_sum) ) ), 0)
	from RegMetrics1 a
)
, RegMetrics3 as (
	select a.*
		, reg3_rsq	= square( a.reg3_r )
		, reg3_cov	= a.reg3_r * a.reg3_x_stdev * a.reg3_y_stdev
	from RegMetrics2 a
)
, RegMetrics4 as (
	select a.*
		, reg3_b	= a.reg3_cov / nullif(a.reg3_x_var, 0)
	from RegMetrics3 a
)
, RegMetrics5 as (
	select a.*
		, reg3_a	= a.reg3_y_bar - ( a.reg3_x_bar * a.reg3_b )
	from RegMetrics4 a
)
, RegMetrics6 as (
	select a.*
		, reg3_pred = ( a.reg3_b * a.x_pct_change_est_avg_price ) + a.reg3_a
		, reg3_resid = a.pct_change_est_avg_price - ( ( a.reg3_b * a.x_pct_change_est_avg_price ) + a.reg3_a )
	from RegMetrics5 a
)
select 
	a.timestamp
	,a.pct_change_est_avg_price
	,a.x_pct_change_est_avg_price
	,a.reg3_pred
	,a.reg3_resid
from RegMetrics6 a
order by a.[timestamp] desc
-- rsq

go