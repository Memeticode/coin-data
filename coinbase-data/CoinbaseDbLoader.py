import datetime as dt
import time
import cbpro
from CoinbaseDb import CoinbaseDb


'''
This class handles loading data from the coinbase api to the coinbase tables in the coindata database
It currently loads all historic data going backwards from a specified poin
It will be updated to check data availability in the db and update to pull in missing data
Can also be used if listener is setup
'''
class CoinbaseDbLoader:    

    # Init API and Db clients
    public_client = cbpro.PublicClient()
    coinDb = CoinbaseDb()

    gran = {
        "1min": 60, 
        "5min": 300, 
        "15min": 900, 
        "1hr": 3600, 
        "6hr": 21600, 
        "1day": 86400
    }

    def load_products_usd(self):

        # Call API to get list of products
        products = self.public_client.get_products()

        # Filter products to those quoted in USD 
        products_usd = list(set([x["base_currency"] for x in products if x["quote_currency"]=="USD"]))
        products_usd.sort()

        # Load products to db
        self.coinDb.add_product(products_usd)


    # Load Historic Data (update date params to make run for datespan)
    def load_historic_rates_from_before(self, base_date, product, granularity):

        last_date = base_date
        max_records_per_call = 200

        product_id_api = "{}-USD".format(product)
        product_fk = self.coinDb.get_product_fk(product)
        granularity_fk = self.coinDb.get_rate_granularity_fk(granularity)

        while True:
            start_date = last_date - (dt.timedelta(seconds=self.gran[granularity]) * max_records_per_call)
            end_date = last_date - dt.timedelta(seconds=1)

            try:
                rates = self.public_client.get_product_historic_rates(
                    product_id=product_id_api, 
                    start=start_date, 
                    end=end_date, 
                    granularity=self.gran[granularity]
                )
            except Exception as e:
                print('Err: ', e.args)
                time.sleep(5)
                print(f'''Retrying get_product_historic_rates(
                    product:{product_id_api}, 
                    start:{start_date}, 
                    end:{end_date}, 
                    gran:{granularity} )''')
                rates = self.public_client.get_product_historic_rates(
                    product_id=product_id_api, 
                    start=start_date, 
                    end=end_date, 
                    granularity=self.gran[granularity]
                )
            
            #Exit when there are no more results
            if len(rates)==0:
                break
            else:
                
                try:
                   self.coinDb.add_product_historic_rates(product_fk, granularity_fk, rates)
                except:
                   print(f"ERROR LOADING:\t{product}\t{granularity}\t{start_date}\t{end_date}")
                #res_tracker.append(res_summary)        
                last_date = start_date
                time.sleep(0.2)
                continue


    def load_all_historic_rates_from_before(self, base_date):
        # Max date for loading
        base_date = dt.datetime(2021, 6, 30)

        for p in self.coinDb.get_product():
            product = p[1]
            print(f"Loading Product: {product}...")
            for g in self.coinDb.get_rate_granularity():
                granularity = g[1]
                print(f" Loading Granularity {granularity}...")
                self.load_historic_rates_from_before(base_date, product, granularity)
                print(" Complete.")

            print(f" {product} Complete.")
            print("----------")
            


    def load_subsequent_historic_rates_from_before(self, start_product_id, base_date):
        # Max date for loading
        for p in self.coinDb.get_product():
            product = p[1]
            if p[0] >= start_product_id:
                print(f"Loading Product: {product}...")
                for g in self.coinDb.get_rate_granularity():
                    granularity = g[1]
                    print(f" Loading Granularity {granularity}...")
                    self.load_historic_rates_from_before(base_date, product, granularity)
                    print(" Complete.")

                print(f" {product} Complete.")
                print("----------")

    def load_product_historic_rates_from_before(self, product, base_date):
        # Max date for loading
        print(f"Loading Product: {product}...")
        for g in self.coinDb.get_rate_granularity():
            granularity = g[1]
            print(f" Loading Granularity {granularity}...")
            self.load_historic_rates_from_before(base_date, product, granularity)
            print(" Complete.")

        print(f" {product} Complete.")
        print("----------")
    
