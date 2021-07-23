
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
