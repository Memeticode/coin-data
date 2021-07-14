/*
Uses coinbase pro api (cbpro)
*/
use CoinData;
go

/* SUPPORT: */

-- Table facilitates calling coinbase api historic rates
-- The seconds column is used to populate parameter granularity when querying Coinbase API historic rates
create table dbo.RateGranularity (
	id int identity(1,1) primary key not null,
	[desc] nvarchar(10) not null unique,
	[seconds] int not null unique
);
go


/* REPLICATES API */

-- Api: public_client.get_products()
-- Conbase Products With Price (loading USD only)
create table dbo.ProductUSD (
	id int identity(1,1) primary key not null, 
	product nvarchar(5) not null unique,
);
go

-- Api: public_client.get_product_historic_rates()
-- Historic pricing over intervals (loading USD only)
create table dbo.ProductHistoricRatesUSD (
	product_fk int not null foreign key references dbo.ProductUSD(id),
	granularity_fk int not null foreign key references dbo.RateGranularity(id),
	[start_datetime] datetime2 not null,
	[timestamp] nvarchar(10) not null,
	[low] decimal(26,8) not null,
	[high] decimal(26,8) not null,
	[open] decimal(26,8) not null,
	[close] decimal(26,8) not null,
	[volume] decimal(26,8) not null,
    constraint PK_ProductHistoricRatesUSD 
        primary key clustered (product_fk, granularity_fk, [timestamp])
);
go
