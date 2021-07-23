use CoinData;
go


select * from dbo.RateGranularity

-- Granularity at 1 day


declare 
 @min_value_to_capture float = 0.1
,@max_loss_to_avoid float	 = -0.02
,@hold_period_seconds int	 = 86400 -- 1 day
;

-- Run Product and Granularity
declare 
@granularity nvarchar(10)	= '6hr'
, @product nvarchar(5)		= 'LINK'
, @granularity_fk int
, @product_fk int;

select @granularity_fk = id from CoinData.dbo.RateGranularity where [desc] = @granularity;
select @product_fk = id from CoinData.dbo.ProductUSD where product = @product;
if (@granularity_fk is null) begin; throw 5100, 'Specified granularity does not exist', 1; end;
if (@product_fk is null) begin; throw 5100, 'Specified product does not exist', 1; end;

WITH
GainOpportunityPoints as (
	-- we want buy points where the price goes up 4% before it goes down 2%!
	select a.*
		, is_gain_opportunity = 
			case when eval_period.hold_through_timestamp is null then null							-- there is not enough future data to evaluate interval 
				when next_loss.[timestamp] is null and next_gain.[timestamp] is null then 0			-- when no future gain / loss is identified 
				when next_loss.[timestamp] is null and next_gain.[timestamp] is not null then 1 	-- when no future loss is identified
				when next_loss.[timestamp] is not null and next_gain.[timestamp] is null then 0		-- when no future gain is identified
				when next_loss.[timestamp] > next_gain.[timestamp] then 1							-- when future gain comes before loss
				when next_loss.[timestamp] < next_gain.[timestamp] then 0							-- when future loss comes before gain
				else 99 end																			-- logic bug
		, next_loss.[timestamp] nlts 
		, next_gain.[timestamp] ngts
		, eval_period.hold_through_timestamp
	from dbo.ProductHistoricRatesUSD_RegressionInputs a
	outer apply ( 
	-- latest point in eval period
		select top 1 [timestamp] as hold_through_timestamp
		from dbo.ProductHistoricRatesUSD_RegressionInputs 
		where product_fk = a.product_fk
		and granularity_fk = a.granularity_fk
		and [timestamp] > a.[timestamp] 
		and cast([timestamp] as bigint) <= cast(a.[timestamp] as bigint) + @hold_period_seconds
		order by [timestamp] desc
	) eval_period
	outer apply ( 
	-- next point where price is at or above our target gain threshold
		select top 1  [timestamp], est_avg_price
			, dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) as gain
		from dbo.ProductHistoricRatesUSD_RegressionInputs 
		where product_fk = a.product_fk
		and granularity_fk = a.granularity_fk
		and [timestamp] > a.[timestamp] 
		and cast([timestamp] as bigint) <= cast(a.[timestamp] as bigint) + @hold_period_seconds
		and est_avg_price > a.est_avg_price
		and dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) >= @min_value_to_capture
		order by [timestamp] asc
	) next_gain
	outer apply ( 
	-- next point where price is at or below our target loss avoidance threshold
		select top 1 [timestamp], est_avg_price
			, dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) as loss
		from dbo.ProductHistoricRatesUSD_RegressionInputs 
		where product_fk = a.product_fk
		and granularity_fk = a.granularity_fk
		and [timestamp] > a.[timestamp] 
		and cast([timestamp] as bigint) <= cast(a.[timestamp] as bigint) + @hold_period_seconds
		and est_avg_price < a.est_avg_price
		and dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) <= @max_loss_to_avoid
		order by [timestamp] asc
	) next_loss
	where a.product_fk = @product_fk
	and a.granularity_fk = @granularity_fk
)
select * 
from GainOpportunityPoints 
order by timestamp desc
;
go

declare
	  @granularity nvarchar(10)		= '1hr'
	, @product nvarchar(5)			= 'LINK'
	, @min_value_to_capture float	= 0.1
	, @max_loss_to_avoid float		= -0.02
	, @hold_period_seconds int		= 86400 -- 1day

select a.start_datetime
	, a.[timestamp]
	, a.is_gain_opportunity
	, a.est_avg_price
	, a.est_avg_trade_volume
	--, isnull(btc.reg3_b,0)			as btc_reg3_b
	--, isnull(btc.reg3_rsq,0)		as btc_reg3_rsq
	--, isnull(btc.reg3_resid,0)		as btc_reg3_resid
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




select * 
from dbo.ProductHistoricRatesUSD_TrendedCorrelation 
where product_fk = 35 and x_product_fk = 14 and granularity_fk = 4
order by timestamp desc
go



ALTER FUNCTION dbo.tfnBuyOpportunities (
	  @granularity nvarchar(10)		--= '6hr'
	, @product nvarchar(5)			--= 'LINK'
	, @min_value_to_capture float	= 0.1
	, @max_loss_to_avoid float		= -0.02
	, @hold_period_seconds int		= 86400 -- 1day
)
RETURNS TABLE
as 
RETURN 

	WITH
	GainOpportunityPoints as (
		-- we want buy points where the price goes up 4% before it goes down 2%!
		select a.*
			, is_gain_opportunity = 
				case when eval_period.hold_through_timestamp is null then null							-- there is not enough future data to evaluate interval 
					when next_loss.[timestamp] is null and next_gain.[timestamp] is null then 0			-- when no future gain / loss is identified 
					when next_loss.[timestamp] is null and next_gain.[timestamp] is not null then 1 	-- when no future loss is identified
					when next_loss.[timestamp] is not null and next_gain.[timestamp] is null then 0		-- when no future gain is identified
					when next_loss.[timestamp] > next_gain.[timestamp] then 1							-- when future gain comes before loss
					when next_loss.[timestamp] < next_gain.[timestamp] then 0							-- when future loss comes before gain
					else 99 end					
		from dbo.ProductHistoricRatesUSD_RegressionInputs a
		outer apply ( 
		-- latest point in eval period
			select top 1 [timestamp] as hold_through_timestamp
			from dbo.ProductHistoricRatesUSD_RegressionInputs 
			where product_fk = a.product_fk
			and granularity_fk = a.granularity_fk
			and [timestamp] > a.[timestamp] 
			and cast([timestamp] as bigint) <= cast(a.[timestamp] as bigint) + @hold_period_seconds
			order by [timestamp] desc
		) eval_period
		outer apply ( 
		-- next point where price is at or above our target gain threshold
			select top 1  [timestamp], est_avg_price
				, dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) as gain
			from dbo.ProductHistoricRatesUSD_RegressionInputs 
			where product_fk = a.product_fk
			and granularity_fk = a.granularity_fk
			and [timestamp] > a.[timestamp] 
			and cast([timestamp] as bigint) <= cast(a.[timestamp] as bigint) + @hold_period_seconds
			and est_avg_price > a.est_avg_price
			and dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) >= @min_value_to_capture
			order by [timestamp] asc
		) next_gain
		outer apply ( 
		-- next point where price is at or below our target loss avoidance threshold
			select top 1 [timestamp], est_avg_price
				, dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) as loss
			from dbo.ProductHistoricRatesUSD_RegressionInputs 
			where product_fk = a.product_fk
			and granularity_fk = a.granularity_fk
			and [timestamp] > a.[timestamp] 
			and cast([timestamp] as bigint) <= cast(a.[timestamp] as bigint) + @hold_period_seconds
			and est_avg_price < a.est_avg_price
			and dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) <= @max_loss_to_avoid
			order by [timestamp] asc
		) next_loss
		where a.product_fk = (select id from CoinData.dbo.ProductUSD where product = @product)
		and a.granularity_fk = (select id from CoinData.dbo.RateGranularity where [desc] = @granularity)
	)
	select * 
	from GainOpportunityPoints 
	where is_gain_opportunity is not null
	--order by timestamp desc
;

declare
	  @granularity nvarchar(10)		= '1hr'
	, @product nvarchar(5)			= 'LINK'
	, @min_value_to_capture float	= 0.1
	, @max_loss_to_avoid float		= -0.02
	, @hold_period_seconds int		= 86400 -- 1day

select * from dbo.tfnBuyOpportunities('1hr', 'LINK', 0.1, -0.2, 86400) order by [timestamp] desc

select * from dbo.ProductUSD

/*
Assets from: https://grayscaleinvest.medium.com/update-grayscale-investments-exploring-additional-assets-e4e80da683bb


-- Layer 1
'ETH'
'ICP'
'ADA'
'DOT'
'SOL'
'XTZ'

-- Crypto currency
'BTC'
'BCH'
'LTC'
'ZEC'


-- Web 3.0
'BAT'
'FIL'

-- Gaming
'MANA'


-- DeFi
'LINK'
'AAVE'
'COMP'



'AAVE'
'ADA'
'ATOM'
'BAT'
'BAND'
'COMP'
'CRV'
'EOS'
'FIL'
'GTC'
'GRT'
'LINK'
'LRC'
'MKR'
'MLN'
'NKN'
'NU'
'OGN'
'RLC'
'SNX'
'SOL'
'QNT'
'XLM'
'YFI'
'ZRX'


'MATIC'

select id, product, min(start_datetime)
from dbo.productusd a
join dbo.producthistoricratesusd b on b.product_fk = a.id
group by id, product
order by 3 asc

*/


