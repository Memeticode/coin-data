from Env import Env
from datetime import datetime
import sqlalchemy as sql


# Data Access Layer
class CoinbaseDb:

    engine = None
    metadata = None
    inspector = None 


    def __init__(self):
        connection_string = "DRIVER={SQL Server};SERVER=DESKTOP-BTN3TLD\TESQL;DATABASE=CoinData;Trusted_Connection=yes;"
        connection_url = sql.engine.url.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

        self.engine = sql.create_engine(connection_url, pool_size=10, max_overflow=0)
        self.metadata = sql.MetaData()
        self.metadata.reflect(self.engine)
        self.inspector = sql.inspect(self.engine)
        # inspector.get_table_names() SHOULD == ['ProductHistoricRatesUSD', 'ProductUSD', 'RateGranularity']

    ### SUPPORT INFO
    def get_rate_granularity(self):   
        with self.engine.connect() as conn:
            tbl = self.metadata.tables["RateGranularity"]
            res_obj = conn.execute(tbl.select().order_by(tbl.c.seconds.desc()))
            res = res_obj.fetchall()
        return res
            
    def get_rate_granularity_fk(self, granularity_desc):        
        with self.engine.connect() as conn:
            tbl = self.metadata.tables["RateGranularity"]
            fk = conn.execute(
                sql.select(tbl.c.id)
                .where(tbl.c.desc==granularity_desc)
            ).first()["id"]
        return fk

    ### API DATA

    ## Product
    # arg products is string list or string
    def add_product(self, products):
        with self.engine.connect() as conn:
            result = conn.execute(
                self.metadata.tables["ProductUSD"].insert(
                    [{"product": p} for p in products]
                )
            )

    def get_product(self):   
        with self.engine.connect() as conn:
            tbl = self.metadata.tables["ProductUSD"]
            res_obj = conn.execute(tbl.select())
            res = res_obj.fetchall()
        return res
            
    def get_product_fk(self, product):        
        with self.engine.connect() as conn:
            tbl = self.metadata.tables["ProductUSD"]
            fk = conn.execute(
                sql.select(tbl.c.id)
                .where(tbl.c.product==product)
            ).first()["id"]
        return fk

    ## Product Historic Rates
    # arg historic_rates is list of arrays returned from get_product_historic_rates
    def add_product_historic_rates(self, product_fk, granularity_fk, historic_rates):
        # could replace fks with args and scalar query
        with self.engine.connect() as conn:
            result = conn.execute(
                self.metadata.tables["ProductHistoricRatesUSD"].insert([
                    {
                        "product_fk": product_fk,
                        "granularity_fk": granularity_fk,
                        "start_datetime": datetime.fromtimestamp(r[0]),
                        "timestamp": r[0],
                        "low": r[1],
                        "high": r[2],
                        "open": r[3],
                        "close": r[4],
                        "volume": r[5],
                    } 
                    for r in historic_rates
                ])
            )
            





