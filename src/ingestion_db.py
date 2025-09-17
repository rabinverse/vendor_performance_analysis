import pandas as pd 
import os 
import time
from sqlalchemy import create_engine

engine= create_engine('sqlite:///inventory_data.db')

def ingest_db(df,table_name,engine):
    df.to_sql(table_name,con=engine,if_exists="replace",index=False)

def load_raw_data():
    for file in os.listdir("../data/raw"): 
        start_time=time.time() 
        df=pd.read_csv('../data/raw/'+file) 
        print(file,':',(time.time()-start_time)) 
        ingest_db(df,file[:-4],engine)

if __name__=="__main__":
   load_raw_data()