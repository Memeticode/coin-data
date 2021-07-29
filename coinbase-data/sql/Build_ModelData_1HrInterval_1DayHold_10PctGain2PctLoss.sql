use CoinData;
go

--drop TABLE dbo.ModelData_1HrInterval_1DayHold_10PctGain2PctLoss
;
go

CREATE TABLE dbo.ModelData_1HrInterval_1DayHold_10PctGain2PctLoss (
	[product_fk] [int] NOT NULL foreign key references dbo.ProductUSD(id)
	, is_gain_opportunity				 bit not null
	, [timestamp]						[nvarchar](10) NOT NULL
	, [start_datetime]					[datetime2] NOT NULL
	, pct_change_est_avg_price			real not null
	, pct_change_est_avg_trade_volume	real not null
	, sma8_pct_diff						real not null
	, sma21_pct_diff					real not null
	, sma55_pct_diff					real not null
	, sma89_pct_diff					real not null
	, sma144_pct_diff					real not null
	, btc_change_est_avg_price			real not null
	, btc_change_est_avg_trade_volume	real not null
	, btc_sma8_pct_diff					real not null
	, btc_sma21_pct_diff				real not null
	, btc_sma55_pct_diff				real not null
	, btc_sma89_pct_diff				real not null
	, btc_sma144_pct_diff				real not null
	, btc_reg3_b						real not null
	, btc_reg3_rsq						real not null
	, btc_reg3_resid					real not null
	, btc_reg8_b						real not null
	, btc_reg8_rsq						real not null
	, btc_reg8_resid					real not null
	, btc_reg21_b						real not null
	, btc_reg21_rsq						real not null
	, btc_reg21_resid					real not null
);
go


SELECT
	-- [product_fk] 
	  is_gain_opportunity				
	--, [timestamp]						
	--, [start_datetime]					
	, pct_change_est_avg_price			
	, pct_change_est_avg_trade_volume	
	, sma8_pct_diff						
	, sma21_pct_diff					
	, sma55_pct_diff					
	, sma89_pct_diff					
	, sma144_pct_diff					
	, btc_change_est_avg_price			
	, btc_change_est_avg_trade_volume	
	, btc_sma8_pct_diff					
	, btc_sma21_pct_diff				
	, btc_sma55_pct_diff				
	, btc_sma89_pct_diff				
	, btc_sma144_pct_diff				
	, btc_reg3_b						
	, btc_reg3_rsq						
	, btc_reg3_resid					
	, btc_reg8_b						
	, btc_reg8_rsq						
	, btc_reg8_resid					
	, btc_reg21_b						
	, btc_reg21_rsq						
	, btc_reg21_resid		
FROM dbo.ModelData_1HrInterval_1DayHold_10PctGain2PctLoss 

);
go


-- Model params
declare 
@granularity nvarchar(10)		= '1hr'
,@hold_granularity nvarchar(10)	= '1day'
,@min_value_to_capture float	= 0.1
,@max_loss_to_avoid float		= -0.02
,@longest_interval_in_model int = 144
,@granularity_fk int
,@granularity_seconds int
,@hold_period_seconds int		
;

select @granularity_fk = id
, @granularity_seconds = [seconds]
from dbo.RateGranularity 
where [desc] = @granularity
;
select @hold_period_seconds = [seconds]
from dbo.RateGranularity 
where [desc] = @hold_granularity
;

WITH 
TradeProducts as (
-- Products in model
	select id, product
	from dbo.ProductUSD
	where product in (
		  'AAVE'
		, 'ADA'
		, 'ATOM'
		, 'BAT'
		, 'BAND'
		, 'COMP'
		, 'CRV'
		, 'EOS'
		, 'FIL'
		, 'GTC'
		, 'GRT'
		, 'LINK'
		, 'LRC'
		, 'MKR'
		, 'MANA'
		, 'MLN'
		, 'NKN'
		, 'NU'
		, 'OGN'
		, 'RLC'
		, 'REN'
		, 'SNX'
		, 'SOL'
		, 'QNT'
		, 'XLM'
		, 'YFI'
		, 'ZRX'
	)
)
, ProductDataStarts as (
	select 
		b.product_fk
		, b.granularity_fk
		, min(b.[timestamp]) as [min_timestamp]
	from TradeProducts a
	JOIN dbo.ProductHistoricRatesUSD b on a.id = b.product_fk
	where b.granularity_fk = @granularity_fk
	group by
		b.product_fk
		, b.granularity_fk
)
, GainOpportunityPoints as (
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
	join ProductDataStarts b
		on a.product_fk = b.product_fk
		and a.granularity_fk = b.granularity_fk
		and cast(a.[timestamp] as bigint) >= cast(b.min_timestamp as bigint) + (@longest_interval_in_model * @granularity_seconds)
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
)
insert into dbo.ModelData_1HrInterval_1DayHold_10PctGain2PctLoss
select 
	a.product_fk
	, a.is_gain_opportunity
	, a.[timestamp]
	, a.start_datetime
	, a.pct_change_est_avg_price
	, a.pct_change_est_avg_trade_volume
	, ma.sma8_pct_diff
	, ma.sma21_pct_diff
	, ma.sma55_pct_diff
	, ma.sma89_pct_diff
	, ma.sma144_pct_diff
	, btc.pct_change_est_avg_price			as btc_change_est_avg_price 
	, btc.pct_change_est_avg_trade_volume	as btc_change_est_avg_trade_volume 
	, btc_ma.sma8_pct_diff					as btc_sma8_pct_diff
	, btc_ma.sma21_pct_diff					as btc_sma21_pct_diff
	, btc_ma.sma55_pct_diff					as btc_sma55_pct_diff
	, btc_ma.sma89_pct_diff					as btc_sma89_pct_diff
	, btc_ma.sma144_pct_diff				as btc_sma144_pct_diff
	, isnull(btc_reg.reg3_b,0)				as btc_reg3_b
	, isnull(btc_reg.reg3_rsq,0)			as btc_reg3_rsq
	, isnull(btc_reg.reg3_resid,0)			as btc_reg3_resid
	, isnull(btc_reg.reg8_b,0)				as btc_reg8_b
	, isnull(btc_reg.reg8_rsq,0)			as btc_reg8_rsq
	, isnull(btc_reg.reg8_resid,0)			as btc_reg8_resid
	, isnull(btc_reg.reg21_b,0)				as btc_reg21_b
	, isnull(btc_reg.reg21_rsq,0)			as btc_reg21_rsq
	, isnull(btc_reg.reg21_resid,0)			as btc_reg21_resid
from GainOpportunityPoints a
join dbo.ProductHistoricRatesUSD_MovingAvgs ma
	on a.[timestamp] = ma.[timestamp]
	and a.granularity_fk = ma.granularity_fk
	and a.product_fk = ma.product_fk
join dbo.ProductHistoricRatesUSD_RegressionInputs btc 
	on a.[timestamp] = btc.[timestamp]
	and a.granularity_fk = btc.granularity_fk
	and btc.product_fk = 14	-- btc fk
join dbo.ProductHistoricRatesUSD_TrendedCorrelation btc_reg
	on a.[timestamp] = btc_reg.[timestamp]
	and a.granularity_fk = btc_reg.granularity_fk
	and a.product_fk = btc_reg.product_fk
	and btc_reg.x_product_fk = 14	-- btc fk
join dbo.ProductHistoricRatesUSD_MovingAvgs btc_ma
	on a.[timestamp] = btc_ma.[timestamp]
	and a.granularity_fk = btc_ma.granularity_fk
	and btc_ma.product_fk = 14	-- btc fk
where a.is_gain_opportunity is not null
order by a.[timestamp] desc
;
go



select 
	b.product_fk, a.product
	, count(*) num_records
	, sum(case when b.is_gain_opportunity = 1 then 1 else 0 end) as num_gain_opp
	, sum(case when b.is_gain_opportunity = 1 then 1.0 else 0 end)/sum(1.0) as pct_gain_opp
	, min(b.[timestamp]) as [min_timestamp]
	, max(b.[timestamp]) as [max_timestamp]
	, min(b.start_datetime) as min_dt
	, max(b.start_datetime) as max_dt
from dbo.ModelData_1HrInterval_1DayHold_10PctGain2PctLoss b
join dbo.ProductUSD a on b.product_fk = a.id
group by grouping sets ((), (b.product_fk, a.product))
order by b.product_fk
;
go

