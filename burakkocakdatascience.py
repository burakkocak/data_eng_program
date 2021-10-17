#!/usr/bin/env python
# coding: utf-8

# In[169]:


import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
import pymysql
import matplotlib.pyplot as plt
import time
import datetime
from dateutil import tz

today = datetime.date.today()
 
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
exc="BITFINEX"

def add_customer(cusno, name):
    # Connect to the database
    connection = pymysql.connect(host='localhost', user='root', password='', db='datasc')
    cursor = connection.cursor()
    # Create a customer
    sql = "INSERT INTO `customer` (`customer_id`, `customer_name`) VALUES (%s, %s)"

    # Execute the query
    cursor.execute(sql, (cusno,name))
    connection.commit()
    
def add_asset(cusno, exchange, asset, balance):
    # Connect to the database
    connection = pymysql.connect(host='localhost', user='root', password='', db='datasc')
    cursor = connection.cursor()
    balance_id= cursor.execute("select max(balance_id) from balance")
    aba = cursor.fetchone()
    balance_id = aba[0] + 1
    sql1="select updated_at from datasc.market where market.updated_at = (select max(market.updated_at) from datasc.market where  base_asset = %(asset)s)  and market.exchange_id = %(exchange)s"
    t_datetime = cursor.execute(sql1,{'asset':asset,'exchange':exchange})
    trans_datetime = cursor.fetchone()
    # add asset to account
    sql = "INSERT INTO `balance` (`balance_id`, `customer_customer_id`,`exchange`,`asset`,`balance`,`trans_datetime`) VALUES (%s,%s,%s,%s,%s,%s)"
    # Execute the query
    cursor.execute(sql, (balance_id,cusno, exchange, asset, balance,trans_datetime[0]))
    connection.commit()
    print("transaction done! %s of your %s from %s added to your balance " %(balance, asset ,exchange))
  


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


def data_check(asset,market):
    print("last 24h information about: " + asset)
    db_connection_str = 'mysql+pymysql://root:@127.0.0.1/datasc'
    db_connection = create_engine(db_connection_str)
    was = int(d1)-1
    sql = 'select exchange_id, base_asset, price, left(updated_at,4) year, right(left(updated_at,7),2) month, right(left(updated_at,10),2) day,right(left(updated_at,13),2) hour,right(left(updated_at,16),2) minute, right(left(updated_at,19),2) sec  from datasc.market where (right(left(updated_at,10),2) >= %(d1)s) order by exchange_id,base_asset,updated_at'
    df = pd.read_sql(sql, con=db_connection,params= {'d1': was})
    
    newdf = df[(df.base_asset == asset) & (df.exchange_id==market) ]
    a=newdf.loc[(newdf["base_asset"] == asset),  ["base_asset","price","minute"]]
    ##print(newdf.head())
    a.plot()

def data_check_h(asset,market):
    print("last 1h + to now information about: " + asset)
    db_connection_str = 'mysql+pymysql://root:@127.0.0.1/datasc'
    db_connection = create_engine(db_connection_str)
    was = int(d1)
    was1 = int(h)-1
    sql = 'select exchange_id, base_asset, price, left(updated_at,4) year, right(left(updated_at,7),2) month, right(left(updated_at,10),2) day,right(left(updated_at,13),2) hour,right(left(updated_at,16),2) minute, right(left(updated_at,19),2) sec  from datasc.market where (right(left(updated_at,10),2) >= %(d1)s) and (right(left(updated_at,13),2)>=%(h)s) order by exchange_id,base_asset,updated_at'
    df = pd.read_sql(sql, con=db_connection,params= {'d1': was, 'h':was1})
    
    newdf = df[(df.base_asset == asset) & (df.exchange_id==market) ]
    a=newdf.loc[(newdf["base_asset"] == asset),  ["base_asset","price","hour","minute"]]
    a.plot()


def last_hour_fall()
    print("last 1h + to now information about: ")
    db_connection_str = 'mysql+pymysql://root:@127.0.0.1/datasc'
    db_connection = create_engine(db_connection_str)
    was = int(d1)
    was1 = int(h)-1
    sql = 'select exchange_id, base_asset, price, left(updated_at,4) year, right(left(updated_at,7),2) month, right(left(updated_at,10),2) day,right(left(updated_at,13),2) hour,right(left(updated_at,16),2) minute, right(left(updated_at,19),2) sec  from datasc.market where (right(left(updated_at,10),2) >= %(d1)s) and (right(left(updated_at,13),2)>=%(h)s) order by exchange_id,base_asset,updated_at'
    df = pd.read_sql(sql, con=db_connection,params= {'d1': was, 'h':was1})





##while True:    
##    try:
##        asset_price()
##        usd_market()
##    except:
##        print ("Error - Data can not be inserted")
##    time.sleep(60)







#print("son bir saatteki fiyatlar :", newdf1)


# In[170]:


data_check('BTC','BITFINEX') 


# In[293]:


add_asset(1, 'COINBASE', 'SOL', 1000)


# In[266]:


data_check_h('SOL','COINBASE')


# In[201]:


def last_hour_fall():
    print("son bir saatte %10 ve daha fazla değer kaybeden varlıklar")
    link = "https://www.cryptingup.com/api/assets?size=10000"

    df= pd.DataFrame(columns=['asset_id','name','price','updated_at','h'])

    if requests.get(link).status_code  == 200:
        response = requests.get(link).json()
        for i in range(0,1000):
            df = df.append({'asset_id':response['assets'][i]['asset_id'],'name':response['assets'][i]['name'],'price':(response['assets'][i]['price']),'updated_at':response['assets'][i]['updated_at'],'h':(response['assets'][i]['change_1h'])},ignore_index=True)
    percent10 = df[(df.h <= -0.1) ]
    print(percent10)


# In[203]:


last_hour_fall()


# In[204]:


def last_day_fall():
    print("son bir gunde %10 ve daha fazla değer kaybeden varlıklar")
    link = "https://www.cryptingup.com/api/assets?size=10000"

    df= pd.DataFrame(columns=['asset_id','name','price','updated_at','d'])

    if requests.get(link).status_code  == 200:
        response = requests.get(link).json()
        for i in range(0,1000):
            df = df.append({'asset_id':response['assets'][i]['asset_id'],'name':response['assets'][i]['name'],'price':(response['assets'][i]['price']),'updated_at':response['assets'][i]['updated_at'],'d':(response['assets'][i]['change_24h'])},ignore_index=True)
    percent10 = df[(df.d <= -0.1) ]
    print(percent10)
               
        


# In[205]:


last_day_fall()


# In[295]:


def pie_chart(cusno):
    label=[]
    size = []
    print(str(cusno) +"hesap nolu musteri hesap durumu")
    sql ="select customer_customer_id, asset, sum(balance) toplam from balance where customer_customer_id = %(cn)s group by customer_customer_id, asset"
    db_connection_str = 'mysql+pymysql://root:@127.0.0.1/datasc'
    db_connection = create_engine(db_connection_str)
    df = pd.read_sql(sql, con=db_connection,params= {'cn':cusno})
    print(df)
    pdf = pd.DataFrame(columns=['asset','total'])
    for a in df.index:
        link = "https://www.cryptingup.com/api/assets?size=10000"
        dfd= pd.DataFrame(columns=['asset_id','price','updated_at'])
        response = requests.get(link).json()
        for i in range(0,1000):
            dfd= dfd.append({'asset_id':response['assets'][i]['asset_id'],'price':(response['assets'][i]['price']),'updated_at':response['assets'][i]['updated_at']},ignore_index=True)
        ast= df.asset[a]
        #print(ast)
        ast_price_df = dfd.loc[ (dfd.asset_id == ast) ]
        ast_price =ast_price_df['price'].values[0]
        #print(ast_price)
        #print(str(df.toplam.values[a]))
        label.append(ast)
        size.append(ast_price * df.toplam.values[a])
    #print(label)
    #print(size)
    fig = plt.figure(figsize =(10, 7))
    plt.pie(size, labels = label,autopct="%1.1f%%")
    plt.title("USD cinsinden portfoy dagilimi")
  
    # show plot
    plt.show()
    
    
        
        
            


# In[296]:


pie_chart(1)


# In[ ]:




