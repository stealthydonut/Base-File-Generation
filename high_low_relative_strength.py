import pandas as pd
import numpy as np
import quandl as qd
import os
import time
import datetime as dt
qd.ApiConfig.api_key ='BVno6pBYgcEvZJ6uctTr'
bigdata = pd.DataFrame()
badlist = pd.DataFrame()
shortintdata = pd.DataFrame()
big = pd.DataFrame()
 
 
tickerlist = ['YAHOO/ZG'],\
['YAHOO/ZN'],\
['YAHOO/ZNWAA'],\
['YAHOO/ZION'],\
['YAHOO/ZIONW'],\
['YAHOO/ZIONZ'],\
['YAHOO/ZIOP'],\
['YAHOO/ZIXI'],\
['YAHOO/ZGNX'],\
['YAHOO/ZSAN'],\
['YAHOO/ZUMZ'],\
['YAHOO/ZYNE'],\
['YAHOO/ZNGA']
 
for i in tickerlist:
    try:
        data = qd.get(i[0])
        data['ticker']= i[0]
        data['index1'] = data.index
        data['dates'] = [dt.datetime(year=d.year, month=d.month, day=d.day) for d in data['index1']]
        data['year'] = data['index1'].dt.strftime("%Y")
        data['month'] = data['index1'].dt.strftime("%m")    
        data['day'] = data['index1'].dt.strftime("%d")
        datax=data.sort_values(['ticker','dates'],ascending=False)
        datax['key_cnt'] = datax.groupby(['ticker']).cumcount() 
        bigdata = bigdata.append(datax, ignore_index=True)
        errortail = data.tail(1)
        error = error.append(errortail)     
        print dt.datetime.now()
    except:
        print i[0] + ' error'  
               

for i in range(0,500):
  
    data=bigdata[bigdata['key_cnt'] >= i] 
    data2=data[data['key_cnt'] < 150 + i]
    data2['value']=i
    data2=data2.sort_values(['ticker','dates'],ascending=True)
      
    #Determine if position is hitting new highs and new lows within a specified time frame
    #Create the index to determine the number of days
  
    #Create time frames to determine if hitting new highs
    #4week Hitting Highs and Hitting Lows
    data2['nh20var'] = np.where(data2['key_cnt']<= 20 + i, 'yes', 'no')
    data2=data2.sort(['ticker','dates'],ascending=True)
    data2['nhmax20'] = data2.groupby(['ticker','nh20var'])['Close'].cummax()
    data2['nh20'] = np.where(data2['nhmax20']==data2['Close'], 1, 0)
    data2['nlmin20'] = data2.groupby(['ticker','nh20var'])['Close'].cummin()
    data2['nl20'] = np.where(data2['nlmin20']==data2['Close'], 1, 0)
    #6week Hitting Highs and Hitting Lows
    data2['nh30var'] = np.where(data2['key_cnt']<= 30 + i, 'yes', 'no')
    data2['nhmax30'] = data2.groupby(['ticker','nh30var'])['Close'].cummax()
    data2['nh30'] = np.where(data2['nhmax30']==data2['Close'], 1, 0)
    data2['nlmin30'] = data2.groupby(['ticker','nh30var'])['Close'].cummin()
    data2['nl30'] = np.where(data2['nlmin30']==data2['Close'], 1, 0)
    #8week Hitting Highs and Hitting Lows
    data2['nh40var'] = np.where(data2['key_cnt']<= 40 + i, 'yes', 'no')
    data2['nhmax40'] = data2.groupby(['ticker','nh40var'])['Close'].cummax()
    data2['nh40'] = np.where(data2['nhmax40']==data2['Close'], 1, 0)
    data2['nlmin40'] = data2.groupby(['ticker','nh40var'])['Close'].cummin()
    data2['nl40'] = np.where(data2['nlmin40']==data2['Close'], 1, 0)
    #30week Hitting Highs and Hitting Lows
    data2['nh150var'] = np.where(data2['key_cnt']<= 150 + i, 'yes', 'no')
    data2['nhmax150'] = data2.groupby(['ticker','nh150var'])['Close'].cummax()
    data2['nh150'] = np.where(data2['nhmax150']==data2['Close'], 1, 0)
    data2['nlmin150'] = data2.groupby(['ticker','nh150var'])['Close'].cummin()
    data2['nl150'] = np.where(data2['nlmin150']==data2['Close'], 1, 0)
        
    #Build basing measures
    #Group by 4,8,30 week
    data2['nh20_cum']=data2.groupby(['ticker','nh20var'])['nh20'].cumsum()    
    data2['nl20_cum']=data2.groupby(['ticker','nh20var'])['nl20'].cumsum()    
    data2['nh40_cum']=data2.groupby(['ticker','nh40var'])['nh40'].cumsum()
    data2['nl40_cum']=data2.groupby(['ticker','nh40var'])['nl40'].cumsum()
    data2['nh150_cum']=data2.groupby(['ticker','nh150var'])['nh150'].cumsum()
    data2['nl150_cum']=data2.groupby(['ticker','nh150var'])['nl150'].cumsum() 
      
  

  
    for i in range(2):
        test2=data2.groupby('ticker').tail(i)
        big=big.append(test2, ignore_index=True)
  
print big