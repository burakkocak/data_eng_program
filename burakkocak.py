#!/usr/bin/env python
# coding: utf-8

# In[115]:


import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
import pymysql
import time
import matplotlib.pyplot as plt
import datetime
from dateutil import tz

today = date.today()
 
# dd/mm/YY
d1 = today.strftime("%d")
d2 = today.strftime("%m")
d3 = today.strftime("%Y")

print("d1 =", d1)
print("d2 =", d2)
print("d3 =", d3)
#h/m
h=datetime.datetime.now(tz=tz.UTC).hour    
m=datetime.datetime.now().minute
print(h)
print(m)
ex="BITFINEX"




def usd_market():
    #market usd cinsinden
    link = "https://www.cryptingup.com/api/assets/USD/markets"

    df= pd.DataFrame(columns=['exchange_id','base_asset','price','updated_at'])

    if requests.get(link).status_code  == 200:
        response = requests.get(link).json()
        for i in range(0,100):
            df = df.append({'exchange_id':response['markets'][i]['exchange_id'],'base_asset':response['markets'][i]['base_asset'],'price':(response['markets'][i]['price']),'updated_at':response['markets'][i]['updated_at']},ignore_index=True)
        # Sorting by column 'Market'
        tableName   = "market"
        dataFrame   = df           
        sqlEngine       = create_engine('mysql+pymysql://root:@127.0.0.1/datasc', pool_recycle=3600)
        dbConnection    = sqlEngine.connect()
        try:
            frame           = dataFrame.to_sql(tableName, dbConnection, if_exists='append');
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:

            print("Table %s updated successfully."%tableName);   

        finally:

            dbConnection.close()
    else:
        print("Error Code "+ str(requests.get(link).status_code) +" :can not get market info")


def asset_price():
    #varlik fiyatlarini yuklemek icin
    link = "https://www.cryptingup.com/api/assets?size=10000"

    df= pd.DataFrame(columns=['asset_id','name','price','updated_at'])

    if requests.get(link).status_code  == 200:
        response = requests.get(link).json()
        for i in range(0,1000):
            df = df.append({'asset_id':response['assets'][i]['asset_id'],'name':response['assets'][i]['name'],'price':(response['assets'][i]['price']),'updated_at':response['assets'][i]['updated_at']},ignore_index=True)
        # Silama kolonu 'asset'
               
        tableName   = "price"
        dataFrame   = df           
        sqlEngine       = create_engine('mysql+pymysql://root:@127.0.0.1/datasc', pool_recycle=3600)
        dbConnection    = sqlEngine.connect()
        try:
            frame           = dataFrame.to_sql(tableName, dbConnection, if_exists='append');
        except ValueError as vx:
            print(vx)
        except Exception as ex:   
            print(ex)
        else:

            print("Table %s updated successfully."%tableName);   

        finally:

            dbConnection.close()
    else:
        print("Error Code "+ str(requests.get(link).status_code) +" :can not get asset info")


db_connection_str = 'mysql+pymysql://root:@127.0.0.1/datasc'
db_connection = create_engine(db_connection_str)

df = pd.read_sql('select exchange_id, base_asset, price, left(updated_at,4) year, right(left(updated_at,7),2) month, right(left(updated_at,10),2) day,right(left(updated_at,13),2) hour,right(left(updated_at,16),2) minute, right(left(updated_at,19),2) sec  from datasc.market order by exchange_id,base_asset,updated_at', con=db_connection)

newdf = df[(df.base_asset == "BTC") & (df.exchange_id==ex)]
a=newdf.loc[(newdf["base_asset"] == "BTC"),  ["base_asset","price","minute"]]
print(newdf)
a.plot()


def add_customer(cusno, name):
    # Connect to the database
    connection = pymysql.connect(host='localhost', user='root', password='', db='datasc')
    cursor = connection.cursor()
    # Create a customer
    sql = "INSERT INTO `customer` (`customer_id`, `customer_name`) VALUES (%s, %s)"

    # Execute the query
    cursor.execute(sql, (cusno,name))
    connection.commit()
    
##add_customer(2,'deneme')
def add_asset(cusno, exchange, asset, balance):
    # Connect to the database
    connection = pymysql.connect(host='localhost', user='root', password='', db='datasc')
    cursor = connection.cursor()
    # add asset to account
    sql = "INSERT INTO `balance` (`balance_id`, `customer_customer_id`,`exchange`,`asset`,`balance`) VALUES (%s, %s,%s,%s,%s)"
    balance_id=cursor.execute("select max(balance_id) from balance")+1
    # Execute the query
    cursor.execute(sql, (balance_id,cusno, exchange, asset, balance))
    connection.commit()
##add_asset(1,'BITFINEX','BTC',5)    



##while True:    
##    try:
##        asset_price()
##        usd_market()
##    except:
##        print ("Error - Data can not be inserted")
##    time.sleep(60)


# In[126]:


##son bir saat degisim
d1a = int(d1)
d1a = d1a-1
print(d1a)
print(d2)
newdf1 = df[(df.base_asset == "BTC") & (df.exchange_id==ex) & (df.day==d1a) & (df.month==d2) & (df.year==d3)]


print("son bir saatteki fiyatlar :", newdf1)


# In[ ]:





# In[ ]:




