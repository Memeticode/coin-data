

/*

dbo.ProductUSD
	-- Crypto products
	-- Scraped from cbpro api


dbo.RateGranularity
	-- Interval granularity (fk for this)
	-- Replicated from from cbpro api


dbo.ModelParamTimespanIntervals
	-- Fib seq
	-- Regression span intervals and moving avgs calculated from this


dbo.ProductHistoricRatesUSD
	-- Historic price and volume info for crypto products
	-- Scraped from cbpro api
	

dbo.ProductHistoricRatesUSD_RegressionInputs
	-- Derived from dbo.ProductHistoricRatesUSD
	-- Est avg, price-volume, and % change in interval for crypto products


dbo.ProductHistoricRatesUSD_MovingAvgs
	-- Derived from dbo.ProductHistoricRatesUSD
	-- Moving avg (and diff w/ current price) for  crypto assets
	-- Calculates SMA of est_avg_price over time intervals: 8, 13, 21, 34, 55, 89, 144, 233


dbo.ProductHistoricRatesUSD_TrendedCorrelation
	-- Derived from dbo.ProductHistoricRatesUSD_MovingAvgs
	-- Regression metrics of each product compared to each other product (at same granularity)
	-- Regression span intervals: 3, 8, 21
	-- Metrics per span are: n, x_bar, x_stdev, y_bar, y_stdev, b, r, rsq, resid


*/



/*

-- Compare each to bitcoin and ethereum only for now

-- 



*/



/*
Assets from: https://grayscaleinvest.medium.com/update-grayscale-investments-exploring-additional-assets-e4e80da683bb

-- Layer 1
'ETH'
'ICP'
'ADA'


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

select id, product, min(start_datetime)
from dbo.productusd a
join dbo.producthistoricratesusd b on b.product_fk = a.id
group by id, product
order by 3 asc

*/

