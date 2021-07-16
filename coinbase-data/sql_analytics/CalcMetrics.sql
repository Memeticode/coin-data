
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

alter function dbo.fnPctChangePrice(@price1 decimal(26,8), @price2 decimal(26,8))
returns float
begin
	return case when @price1 is null then null
				when @price1 =0 then 0 
				else (@price2 - @price1) / @price1 end
end
go

*/


select dbo.fnPctChangePrice(52.54025000, 55.75625000)

/*

-- VWAP

-- 



*/

-- Strategy Goal Params
declare 
@min_value_to_capture decimal(6,4)	= 0.04
,@max_loss_to_avoid decimal(6,4)	= 0.02

-- Run Product and Granularity
declare 
@granularity nvarchar(10)	= '1day'
, @product nvarchar(5)		= 'FORTH'
, @granularity_fk int
, @product_fk int;

select @granularity_fk = id from CoinData.dbo.RateGranularity where [desc] = @granularity;
select @product_fk = id from CoinData.dbo.ProductUSD where product = @product;
if (@granularity_fk is null) begin; throw 5100, 'Specified granularity does not exist', 1; end;
if (@product_fk is null) begin; throw 5100, 'Specified product does not exist', 1; end;

select count(*) 
	from CoinData.dbo.ProductHistoricRatesUSD a
	where a.product_fk = @product_fk
	and a.granularity_fk = @granularity_fk
;

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
	where a.product_fk = @product_fk
	and a.granularity_fk = @granularity_fk
)
, PriceChange as (
	select a.*
		-- ideal buy and sell points
		, next_est_avg_price = lead(a.est_avg_price) over(partition by a.product_fk, a.granularity_fk order by a.[timestamp] asc)
		, prev_est_avg_price = lag(a.est_avg_price) over(partition by a.product_fk, a.granularity_fk order by a.[timestamp] asc)
	from EstAvgPrice a
) 
, LogPriceChange as (
	select a.*
		, log(a.est_avg_price) as log_est_avg_price
	from PriceChange a
)
, OptimalBuySellPoints as (
	select a.*
		, is_optimal_buy_point = 
			case when a.next_est_avg_price is null then null
				when a.next_est_avg_price > a.est_avg_price
					and ( a.prev_est_avg_price > a.est_avg_price or a.prev_est_avg_price is null )
				then 1 else 0 end
		, is_optimal_sell_point = 
			case when a.next_est_avg_price is null then null
				when a.next_est_avg_price < a.est_avg_price
					and ( a.prev_est_avg_price < a.est_avg_price or a.prev_est_avg_price is null )
				then 1 else 0 end
	from PriceChange a
)
, GainOpportunityPoints as (
	-- we want buy points where the price goes up 4% before it goes down 2%!
	select a.*
		, is_gain_opportunity = 
			case when next_est_avg_price is null then null
				when next_loss.[timestamp] is null or next_gain.[timestamp] < next_loss.[timestamp] then 1 else 0 end
	from OptimalBuySellPoints a
	outer apply ( -- next point where price is at or above our target gain threshold
		select top 1  [timestamp], est_avg_price
			, dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) as gain
		from OptimalBuySellPoints 
		where product_fk = a.product_fk
		and granularity_fk = a.granularity_fk
		and [timestamp] > a.[timestamp] 
		and est_avg_price > a.est_avg_price
		and dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) >= @min_value_to_capture
		order by [timestamp] asc
	) next_gain
	outer apply ( -- next point where price is at or below our target loss avoidance threshold
		select top 1 [timestamp], est_avg_price
			, dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) as loss
		from OptimalBuySellPoints 
		where product_fk = a.product_fk
		and granularity_fk = a.granularity_fk
		and [timestamp] > a.[timestamp] 
		and est_avg_price < a.est_avg_price
		and dbo.fnPctChangePrice(a.est_avg_price, est_avg_price) <= @max_loss_to_avoid
		order by [timestamp] asc
	) next_loss
)
select * from GainOpportunityPoints
--)
--, RelevantBuySellPoints as (
--	select a.*
--		, case when b.[timestamp] is not null then 1 else 0 end as flag_buy_point
--	from TargetBuySellPoints a
--	left join TargetBuySellPoints b on a.product_fk = b.product_fk and a.granularity_fk = b.granularity_fk and a.[timestamp] = b.[timestamp]
--) 
--select * 
----from RelevantBuyPoints a
--from RelevantBuySellPoints a
--order by a.[timestamp] desc

--	, change_in_next_interval = dbo.fnPctChangePrice(a.est_avg_price, a.next_est_avg_price)
--	where a.is_optimal_buy_point
--	and 
--)
--, RelevantSellPoints

-- get next sell point where value is >4% higher than current buy point
-- get next buy point where value is >2% lower than current buy point

-- select count(*) from (
---- 267
--select a.*
--	, change_from_prev_interval = dbo.fnPctChangePrice(a.prev_est_avg_price, a.est_avg_price)
--	, change_in_next_interval = dbo.fnPctChangePrice(a.est_avg_price, a.next_est_avg_price)
--from PriceChange a
----order by [timestamp] desc
--) a


--find all points where there is a +4% increase before a -3% decrease

--get the row id of the next row which meets these params