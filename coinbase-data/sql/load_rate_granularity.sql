
use CoinData;
go

insert into dbo.RateGranularity ([desc], [seconds]) values 
('1min', 60), 
('5min', 300), 
('15min', 900), 
('1hr', 3600), 
('6hr', 21600), 
('1day', 86400);
go
