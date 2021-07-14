
import datetime as dt
from CoinbaseDbLoader import CoinbaseDbLoader


'''
Assumes tables have been created via scripts in coinbase-data\sql
    1. build_schema.sql
    2. load_rate_granularity.sql
'''

base_date = dt.datetime(2021, 6, 30)
dataLoader = CoinbaseDbLoader()

# Loads all products to product table
dataLoader.load_products_usd()

# Iterates through all products and granularities
dataLoader.load_all_historic_rates_from_before(base_date)