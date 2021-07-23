
use CoinData;
go


select a.product_fk
--, a.granularity_fk
, count(*) cnt
, count(distinct a.granularity_fk) as dist_x_product_fk
, count(distinct a.x_product_fk) as dist_x_product_fk
from dbo.ProductHistoricRatesUSD_TrendedCorrelation a
group by a.product_fk
--, a.granularity_fk
order by a.product_fk
;
go


-- Identify last run product and granularity

select a.product_fk
--, a.granularity_fk
, count(*) cnt
, count(distinct a.granularity_fk) as dist_granularity_fk
, min(a.granularity_fk) as min_granularity_fk
, max(a.granularity_fk) as max_granularity_fk
, count(distinct a.x_product_fk) as dist_x_product_fk
, min(a.x_product_fk) as min_x_product_fk
, max(a.x_product_fk) as max_x_product_fk
from dbo.ProductHistoricRatesUSD_TrendedCorrelation a
group by a.product_fk
--, a.granularity_fk
order by a.product_fk
;
go

select a.product_fk
, a.granularity_fk
, count(*) cnt
, count(distinct a.x_product_fk) as dist_x_product_fk
, min(a.x_product_fk) as min_x_product_fk
, max(a.x_product_fk) as max_x_product_fk
from dbo.ProductHistoricRatesUSD_TrendedCorrelation a
where a.product_fk = 28
group by a.product_fk
, a.granularity_fk
order by a.product_fk
, a.granularity_fk
;
go

select a.product_fk
, a.granularity_fk
, a.x_product_fk
, count(*) cnt
, min(a.start_datetime) as min_dt
, max(a.start_datetime) as max_dt
from dbo.ProductHistoricRatesUSD_TrendedCorrelation a
where a.product_fk = 28
and granularity_fk = 2
group by a.product_fk
, a.granularity_fk
, a.x_product_fk
order by a.product_fk
, a.granularity_fk
, a.x_product_fk
;
go




-- Run Product and Granularity

declare 
@granularity nvarchar(10)
, @product nvarchar(5)	
, @granularity_fk int
, @product_fk int;


declare ProductGranularity cursor static for
	select a.product_fk, a.granularity_fk, b.product, c.[desc]
	from dbo.ProductHistoricRatesUSD a
	join dbo.ProductUSD b on a.product_fk = b.id
	join dbo.RateGranularity c on a.granularity_fk = c.id
	-- added where clause for re-run
	where a.product_fk >= 28
	and case when a.product_fk = 28 and granularity_fk <= 1 then 1
			when a.product_fk = 28 and granularity_fk > 1 then 0
			else 1 end = 1
	group by a.product_fk, a.granularity_fk, b.product, c.[desc]
	order by 1,2 desc

open ProductGranularity
fetch next from ProductGranularity into @product_fk, @granularity_fk, @product, @granularity
while @@FETCH_STATUS = 0
begin
	begin
		print('Product: '+@product+' '+cast(@product_fk as nvarchar(10))+', Granularity: '+@granularity+' '+cast(@granularity_fk as nvarchar(10)));
	end
	begin
		exec dbo.uspLoad_ProductHistoricRatesUSD_TrendedCorrelation @product_fk, @granularity_fk;
	end
	begin
		print('Complete')
	end
	fetch next from ProductGranularity into @product_fk, @granularity_fk, @product, @granularity
end;
close ProductGranularity;
deallocate ProductGranularity;
go


select a.product_fk
, a.granularity_fk
, count(distinct a.x_product_fk) as dist_x_product_fk
, count(*) cnt
from dbo.ProductHistoricRatesUSD_TrendedCorrelation a
group by a.product_fk
, a.granularity_fk
;
go

--Msg 2627, Level 14, State 1, Procedure dbo.uspLoad_ProductHistoricRatesUSD_TrendedCorrelation, Line 5 [Batch Start Line 19]
--Violation of UNIQUE KEY constraint 'UQ__ProductH__07EAFFFADC7CB01C'. Cannot insert duplicate key in object 'dbo.ProductHistoricRatesUSD_TrendedCorrelation'. 
-- The duplicate key value is (1, 2, 1, 1617984480).
--The statement has been terminated.
--Complete
--Product: 1INCH, Granularity: 1hr
--Msg 2627, Level 14, State 1, Procedure dbo.uspLoad_ProductHistoricRatesUSD_TrendedCorrelation, Line 5 [Batch Start Line 19]
--Violation of UNIQUE KEY constraint 'UQ__ProductH__07EAFFFADC7CB01C'. Cannot insert duplicate key in object 'dbo.ProductHistoricRatesUSD_TrendedCorrelation'. The duplicate key value is (1, 2, 1, 1617984480).
--The statement has been terminated.
--Complete