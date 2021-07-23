
-- Test Dataset #1

declare
	  @granularity nvarchar(10)		= '1hr'
	, @product nvarchar(5)			= 'LINK'
	, @min_value_to_capture float	= 0.1
	, @max_loss_to_avoid float		= -0.02
	, @hold_period_seconds int		= 86400 -- 1day

select 
	  a.is_gain_opportunity
--	, a.est_avg_price
	, a.pct_change_est_avg_price
	, a.pct_change_est_avg_trade_volume
	, isnull(btc.reg8_b,0)			as btc_reg8_b
	, isnull(btc.reg8_rsq,0)		as btc_reg8_rsq
	, isnull(btc.reg8_resid,0)		as btc_reg8_resid
	, isnull(btc.reg21_b,0)			as btc_reg21_b
	, isnull(btc.reg21_rsq,0)		as btc_reg21_rsq
	, isnull(btc.reg21_resid,0)		as btc_reg21_resid
	, ma.sma8_pct_diff
	, ma.sma21_pct_diff
	, ma.sma55_pct_diff
	, ma.sma89_pct_diff
	, ma.sma144_pct_diff
	, btc_ma.sma8_pct_diff
	, btc_ma.sma21_pct_diff
	, btc_ma.sma55_pct_diff
	, btc_ma.sma89_pct_diff
	, btc_ma.sma144_pct_diff
from dbo.tfnBuyOpportunities(	-- label dataset
	  @granularity 
	, @product
	, @min_value_to_capture 
	, @max_loss_to_avoid
	, @hold_period_seconds
) a
left join dbo.ProductHistoricRatesUSD_TrendedCorrelation btc 
	on a.[timestamp] = btc.[timestamp]
	and a.granularity_fk = btc.granularity_fk
	and a.product_fk = btc.product_fk
	and btc.x_product_fk = 14	-- btc fk
left join dbo.ProductHistoricRatesUSD_MovingAvgs ma
	on a.[timestamp] = ma.[timestamp]
	and a.granularity_fk = ma.granularity_fk
	and a.product_fk = ma.product_fk
left join dbo.ProductHistoricRatesUSD_MovingAvgs btc_ma
	on a.[timestamp] = btc_ma.[timestamp]
	and a.granularity_fk = btc_ma.granularity_fk
	and btc_ma.product_fk = 14	-- btc fk
order by a.[timestamp] desc

go



-- Test Dataset #2

declare
	  @granularity nvarchar(10)		= '1hr'
	, @product nvarchar(5)			= 'LINK'
	, @min_value_to_capture float	= 0.1
	, @max_loss_to_avoid float		= -0.02
	, @hold_period_seconds int		= 86400 -- 1day

select 
	  a.is_gain_opportunity
	, a.pct_change_est_avg_price
	, a.pct_change_est_avg_trade_volume
	, ma.sma8_pct_diff
	, ma.sma21_pct_diff
	, ma.sma55_pct_diff
	, ma.sma89_pct_diff
	, ma.sma144_pct_diff
-- BTC
	, isnull(btc.reg8_b,0)			as btc_reg8_b
	, isnull(btc.reg8_rsq,0)		as btc_reg8_rsq
	, isnull(btc.reg8_resid,0)		as btc_reg8_resid
	, isnull(btc.reg21_b,0)			as btc_reg21_b
	, isnull(btc.reg21_rsq,0)		as btc_reg21_rsq
	, isnull(btc.reg21_resid,0)		as btc_reg21_resid
	, btc_ma.sma8_pct_diff
	, btc_ma.sma21_pct_diff
	, btc_ma.sma55_pct_diff
	, btc_ma.sma89_pct_diff
	, btc_ma.sma144_pct_diff
-- ETH
	, isnull(eth.reg8_b,0)			as eth_reg8_b
	, isnull(eth.reg8_rsq,0)		as eth_reg8_rsq
	, isnull(eth.reg8_resid,0)		as eth_reg8_resid
	, isnull(eth.reg21_b,0)			as eth_reg21_b
	, isnull(eth.reg21_rsq,0)		as eth_reg21_rsq
	, isnull(eth.reg21_resid,0)		as eth_reg21_resid
	, eth_ma.sma8_pct_diff
	, eth_ma.sma21_pct_diff
	, eth_ma.sma55_pct_diff
	, eth_ma.sma89_pct_diff
	, eth_ma.sma144_pct_diff
from dbo.tfnBuyOpportunities(	-- label dataset
	  @granularity 
	, @product
	, @min_value_to_capture 
	, @max_loss_to_avoid
	, @hold_period_seconds
) a
left join dbo.ProductHistoricRatesUSD_MovingAvgs ma
	on a.[timestamp] = ma.[timestamp]
	and a.granularity_fk = ma.granularity_fk
	and a.product_fk = ma.product_fk
left join dbo.ProductHistoricRatesUSD_TrendedCorrelation btc 
	on a.[timestamp] = btc.[timestamp]
	and a.granularity_fk = btc.granularity_fk
	and a.product_fk = btc.product_fk
	and btc.x_product_fk = 14	-- btc fk
left join dbo.ProductHistoricRatesUSD_MovingAvgs btc_ma
	on a.[timestamp] = btc_ma.[timestamp]
	and a.granularity_fk = btc_ma.granularity_fk
	and btc_ma.product_fk = 14	-- btc fk
left join dbo.ProductHistoricRatesUSD_TrendedCorrelation eth 
	on a.[timestamp] = eth.[timestamp]
	and a.granularity_fk = eth.granularity_fk
	and a.product_fk = eth.product_fk
	and eth.x_product_fk = 27	-- eth fk
left join dbo.ProductHistoricRatesUSD_MovingAvgs eth_ma
	on a.[timestamp] = eth_ma.[timestamp]
	and a.granularity_fk = eth_ma.granularity_fk
	and eth_ma.product_fk = 27	-- eth fk
where a.pct_change_est_avg_price is not null
and a.pct_change_est_avg_trade_volume is not null
order by a.[timestamp] desc

go



-- Test Dataset #3

declare
	  @granularity nvarchar(10)		= '1hr'
	, @product nvarchar(5)			= 'LINK'
	, @min_value_to_capture float	= 0.1
	, @max_loss_to_avoid float		= -0.02
	, @hold_period_seconds int		= 86400 -- 1day

select top (10000)
	  a.is_gain_opportunity
	, a.pct_change_est_avg_price
	, a.pct_change_est_avg_trade_volume
	, ma.sma8_pct_diff
	, ma.sma21_pct_diff
-- BTC
	, isnull(btc.reg8_b,0)			as btc_reg8_b
	, isnull(btc.reg8_rsq,0)		as btc_reg8_rsq
	, isnull(btc.reg8_resid,0)		as btc_reg8_resid
	, isnull(btc.reg21_b,0)			as btc_reg21_b
	, isnull(btc.reg21_rsq,0)		as btc_reg21_rsq
	, isnull(btc.reg21_resid,0)		as btc_reg21_resid
	, btc_ma.sma8_pct_diff
	, btc_ma.sma21_pct_diff

from dbo.tfnBuyOpportunities(	-- label dataset
	  @granularity 
	, @product
	, @min_value_to_capture 
	, @max_loss_to_avoid
	, @hold_period_seconds
) a
join dbo.ProductHistoricRatesUSD_MovingAvgs ma
	on a.[timestamp] = ma.[timestamp]
	and a.granularity_fk = ma.granularity_fk
	and a.product_fk = ma.product_fk
join dbo.ProductHistoricRatesUSD_TrendedCorrelation btc 
	on a.[timestamp] = btc.[timestamp]
	and a.granularity_fk = btc.granularity_fk
	and a.product_fk = btc.product_fk
	and btc.x_product_fk = 14	-- btc fk
join dbo.ProductHistoricRatesUSD_MovingAvgs btc_ma
	on a.[timestamp] = btc_ma.[timestamp]
	and a.granularity_fk = btc_ma.granularity_fk
	and btc_ma.product_fk = 14	-- btc fk
where a.pct_change_est_avg_price is not null
and a.pct_change_est_avg_trade_volume is not null
order by a.[timestamp] desc

go


-- Test Dataset #4
-- vs. btc, regression spans 3, 8, 21
declare
	  @granularity nvarchar(10)		= '1hr'
	, @product nvarchar(5)			= 'LINK'
	, @min_value_to_capture float	= 0.1
	, @max_loss_to_avoid float		= -0.02
	, @hold_period_seconds int		= 86400 -- 1day

select top 17400
	  a.is_gain_opportunity
--	, a.est_avg_price
	, a.pct_change_est_avg_price
	, a.pct_change_est_avg_trade_volume
	, isnull(btc.reg3_b,0)			as btc_reg3_b
	, isnull(btc.reg3_rsq,0)		as btc_reg3_rsq
	, isnull(btc.reg3_resid,0)		as btc_reg3_resid
	, isnull(btc.reg8_b,0)			as btc_reg8_b
	, isnull(btc.reg8_rsq,0)		as btc_reg8_rsq
	, isnull(btc.reg8_resid,0)		as btc_reg8_resid
	, isnull(btc.reg21_b,0)			as btc_reg21_b
	, isnull(btc.reg21_rsq,0)		as btc_reg21_rsq
	, isnull(btc.reg21_resid,0)		as btc_reg21_resid
	, ma.sma8_pct_diff
	, ma.sma21_pct_diff
	, ma.sma55_pct_diff
	, ma.sma89_pct_diff
	, ma.sma144_pct_diff
	, btc_ma.sma8_pct_diff
	, btc_ma.sma21_pct_diff
	, btc_ma.sma55_pct_diff
	, btc_ma.sma89_pct_diff
	, btc_ma.sma144_pct_diff
from dbo.tfnBuyOpportunities(	-- label dataset
	  @granularity 
	, @product
	, @min_value_to_capture 
	, @max_loss_to_avoid
	, @hold_period_seconds
) a
left join dbo.ProductHistoricRatesUSD_TrendedCorrelation btc 
	on a.[timestamp] = btc.[timestamp]
	and a.granularity_fk = btc.granularity_fk
	and a.product_fk = btc.product_fk
	and btc.x_product_fk = 14	-- btc fk
left join dbo.ProductHistoricRatesUSD_MovingAvgs ma
	on a.[timestamp] = ma.[timestamp]
	and a.granularity_fk = ma.granularity_fk
	and a.product_fk = ma.product_fk
left join dbo.ProductHistoricRatesUSD_MovingAvgs btc_ma
	on a.[timestamp] = btc_ma.[timestamp]
	and a.granularity_fk = btc_ma.granularity_fk
	and btc_ma.product_fk = 14	-- btc fk
order by a.[timestamp] desc

go


-- Test Dataset #4
-- vs. btc, regression spans 3, 8, 21
declare
	  @granularity nvarchar(10)		= '1hr'
	, @product nvarchar(5)			= 'LINK'
	, @min_value_to_capture float	= 0.1
	, @max_loss_to_avoid float		= -0.02
	, @hold_period_seconds int		= 86400 -- 1day

select top 17400
	  a.is_gain_opportunity
--	, a.est_avg_price
	, a.pct_change_est_avg_price
	, a.pct_change_est_avg_trade_volume
	, isnull(btc.reg3_b,0)			as btc_reg3_b
	, isnull(btc.reg3_rsq,0)		as btc_reg3_rsq
	, isnull(btc.reg3_resid,0)		as btc_reg3_resid
	, isnull(btc.reg8_b,0)			as btc_reg8_b
	, isnull(btc.reg8_rsq,0)		as btc_reg8_rsq
	, isnull(btc.reg8_resid,0)		as btc_reg8_resid
	, isnull(btc.reg21_b,0)			as btc_reg21_b
	, isnull(btc.reg21_rsq,0)		as btc_reg21_rsq
	, isnull(btc.reg21_resid,0)		as btc_reg21_resid
	, ma.sma8_pct_diff
	, ma.sma21_pct_diff
	, ma.sma55_pct_diff
	, ma.sma89_pct_diff
	, ma.sma144_pct_diff
	, btc_ma.sma8_pct_diff
	, btc_ma.sma21_pct_diff
	, btc_ma.sma55_pct_diff
	, btc_ma.sma89_pct_diff
	, btc_ma.sma144_pct_diff
from dbo.tfnBuyOpportunities(	-- label dataset
	  @granularity 
	, @product
	, @min_value_to_capture 
	, @max_loss_to_avoid
	, @hold_period_seconds
) a
left join dbo.ProductHistoricRatesUSD_TrendedCorrelation btc 
	on a.[timestamp] = btc.[timestamp]
	and a.granularity_fk = btc.granularity_fk
	and a.product_fk = btc.product_fk
	and btc.x_product_fk = 14	-- btc fk
left join dbo.ProductHistoricRatesUSD_MovingAvgs ma
	on a.[timestamp] = ma.[timestamp]
	and a.granularity_fk = ma.granularity_fk
	and a.product_fk = ma.product_fk
left join dbo.ProductHistoricRatesUSD_MovingAvgs btc_ma
	on a.[timestamp] = btc_ma.[timestamp]
	and a.granularity_fk = btc_ma.granularity_fk
	and btc_ma.product_fk = 14	-- btc fk
order by a.[timestamp] desc

go
