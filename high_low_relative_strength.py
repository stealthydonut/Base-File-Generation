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
data['Close_lag1']=data['Close'].shift(1)
data['difference'] = data['Close']-data['Close_lag1']
data['gain'] = np.where(data['difference']>=0, data['difference'],0)
data['loss'] = np.where(data['difference']<0, data['difference'],0)
#Develop 14day Relative Strength Metrics
data['rs14g']=data.groupby('ticker')['gain'].apply(pd.rolling_mean, 14, min_periods=1)
data['rs14l']=data.groupby('ticker')['loss'].apply(pd.rolling_mean, 14, min_periods=1)
data['rs14']=data2['rs14g']/(-1*data['rs14l'])
data['rsi14']= 100-(100/(1+data['rs14']))
#Develop Index Relative Strength Metrics - Calculate Gain and Loss on ticker
data['differenceind'] = data['Index_Close']-data['Index_Close_lag1']
data['gainind'] = np.where(data['differenceind']>=0, data['differenceind'],0)
data['lossind'] = np.where(data['differenceind']<0, data['differenceind'],0)
#Develop 14day Relative Strength Metrics
data['rs14gind']=data.groupby('ticker')['gainind'].apply(pd.rolling_mean, 14, min_periods=1)
data['rs14lind']=data.groupby('ticker')['lossind'].apply(pd.rolling_mean, 14, min_periods=1)
data['rs14ind']=data['rs14gind']/(-1*data['rs14lind'])
data['rsi14ind']= 100-(100/(1+data['rs14ind']))   
print data.dtypes 
print data 













 
#Get the S&P500 index to merge
dataindex=qd.get(["YAHOO/INDEX_GSPC"])  
  
  
dataindex['index1'] = dataindex.index
dataindex['dates'] = [dt.datetime(year=d.year, month=d.month, day=d.day) for d in dataindex['index1']]  
dataindex['Index_Close']=dataindex['YAHOO/INDEX_GSPC - Close']  
dataindex['Index_Open']=dataindex['YAHOO/INDEX_GSPC - Open']
dataindex['Index_High']=dataindex['YAHOO/INDEX_GSPC - High']
dataindex['Index_Low']=dataindex['YAHOO/INDEX_GSPC - Low']
dataindex['Index_Volume']=dataindex['YAHOO/INDEX_GSPC - Volume']
#Develop the lag variable to generate the advance decline metrics  
dataindex['Index_Close_lag1']=dataindex['Index_Close'].shift(1)
dataindex['Index_Close_lag10']=dataindex['Index_Close'].shift(10)
dataindex['Index_Close_lag20']=dataindex['Index_Close'].shift(20)
dataindex['Index_Close_lag40']=dataindex['Index_Close'].shift(40)
dataindex['Index_Close_lag150']=dataindex['Index_Close'].shift(150)
  
dataindexgold=dataindex[['dates','Index_Close','Index_Open','Index_High','Index_Low','Index_Volume','Index_Close_lag1','Index_Close_lag10','Index_Close_lag20','Index_Close_lag40','Index_Close_lag150']]  
#Merge the index with the tickers  
bigdatax=pd.merge(bigdata, dataindexgold, left_on='dates', right_on='dates')
print bigdatax
  
  
  
#The first loop drops rows, and the second loop selects the universe
  
#bigdata2 = bigdata[['Close','key_cnt','ticker','dates']]
#test=bigdata[bigdata['key_cnt'] < 3]
#print test
#The first loop gets the complete universe of the analysis based on key_cnt
#the <150> is the max number of days needed to calculate the values - the range(a,b) <b> is the number of times that you would like to iterate through and calculate the metrics
for i in range(0,500):
  
    data=bigdatax[bigdatax['key_cnt'] >= i] 
    data2=data[data['key_cnt'] < 150 + i]
    data2['value']=i
    data2=data2.sort_values(['ticker','dates'],ascending=True)
      
    #Develop the methodology to build the metrics  
    #Begin Building the analytical file for moving averages and above below moving average
    #Moving Average at the ticker level
    data2['ma10']=data2.groupby('ticker')['Close'].apply(pd.rolling_mean, 10, min_periods=1)
    data2['ma20']=data2.groupby('ticker')['Close'].apply(pd.rolling_mean, 20, min_periods=1)
    data2['ma40']=data2.groupby('ticker')['Close'].apply(pd.rolling_mean, 40, min_periods=1)
    data2['ma150']=data2.groupby('ticker')['Close'].apply(pd.rolling_mean, 150, min_periods=1)
    data2['ma150_dir'] = np.where(data2['ma150']>=data2['Close'], 'above', 'below')  
    data2['ma10_lag1']=data2['ma10'].shift(1)
    data2['ma10_lag10']=data2['ma10'].shift(10)
    data2['ma150_lag1']=data2['ma150'].shift(1)
    data2['ma150_lag10']=data2['ma150'].shift(10) 
      
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
      
  
    #Develop the lag variable to generate the advance decline metrics
          
    #Develop Advance & Decline Metrics
    data2['Close_lag1']=data2['Close'].shift(1)
    data2['Close_lag10']=data2['Close'].shift(10)
    data2['Close_lag20']=data2['Close'].shift(20)
    data2['Close_lag40']=data2['Close'].shift(40)
    data2['Close_lag150']=data2['Close'].shift(150)
    data2['ad1']  = np.where(data2['Close']>data2['Close_lag1'], 1, 0)
    data2['a10']  = np.where(data2['Close']>data2['Close_lag10'], 1, 0)
    data2['a20']  = np.where(data2['Close']>data2['Close_lag20'], 1, 0)
    data2['a40']  = np.where(data2['Close']>data2['Close_lag40'], 1, 0)
    data2['a150'] = np.where(data2['Close']>data2['Close_lag150'], 1, 0)
    data2['d1']   = np.where(data2['Close']<data2['Close_lag1'], 1, 0)
    data2['d10']  = np.where(data2['Close']<data2['Close_lag10'], 1, 0)
    data2['d20']  = np.where(data2['Close']<data2['Close_lag20'], 1, 0)
    data2['d40']  = np.where(data2['Close']<data2['Close_lag40'], 1, 0)
    data2['d150'] = np.where(data2['Close']<data2['Close_lag150'], 1, 0)
     
    #data2=data.sort_values(['ticker','dates'],ascending=False)
    #data2['key_cnt'] = data.groupby(['ticker']).cumcount()  
    #Develop Relative Strength Metrics - Calculate Gain and Loss on ticker
    data2['difference'] = data2['Close']-data2['Close_lag1']
    data2['gain'] = np.where(data2['difference']>=0, data2['difference'],0)
    data2['loss'] = np.where(data2['difference']<0, data2['difference'],0)
    #Develop 14day Relative Strength Metrics
    data2['rs14g']=data2.groupby('ticker')['gain'].apply(pd.rolling_mean, 14, min_periods=1)
    data2['rs14l']=data2.groupby('ticker')['loss'].apply(pd.rolling_mean, 14, min_periods=1)
    data2['rs14']=data2['rs14g']/(-1*data2['rs14l'])
    data2['rsi14']= 100-(100/(1+data2['rs14']))
    #Develop Index Relative Strength Metrics - Calculate Gain and Loss on ticker
    data2['differenceind'] = data2['Index_Close']-data2['Index_Close_lag1']
    data2['gainind'] = np.where(data2['differenceind']>=0, data2['differenceind'],0)
    data2['lossind'] = np.where(data2['differenceind']<0, data2['differenceind'],0)
    #Develop 14day Relative Strength Metrics
    data2['rs14gind']=data2.groupby('ticker')['gainind'].apply(pd.rolling_mean, 14, min_periods=1)
    data2['rs14lind']=data2.groupby('ticker')['lossind'].apply(pd.rolling_mean, 14, min_periods=1)
    data2['rs14ind']=data2['rs14gind']/(-1*data2['rs14lind'])
    data2['rsi14ind']= 100-(100/(1+data2['rs14ind']))   
  
  
    for i in range(2):
        test2=data2.groupby('ticker').tail(i)
        big=big.append(test2, ignore_index=True)
  
  
big=big.sort(['value','ticker','dates'],ascending=False)
#Get the relational data
df = pd.read_csv('C:\Users\davking\Desktop\Personal\company_list_test.csv')
#df['Shares'] = df['MarketCap']/df['LastSale']
#
big2=pd.merge(df, big, left_on='YAHOO_Ticker', right_on='ticker')
 
#Develop Consolidation Zone Methodology
#Lag to get the midpoint
big2['cons20maxlag10']=big2['nhmax20'].shift(10)
big2['cons20minlag10']=big2['nlmin20'].shift(10)
#Calculate the zone position to associate the correct value
#print big2
#Need to determine the consolidation (high and low values)
big2['Rank'] = big2.sort_values(['ticker','dates'],ascending=[True,True]).groupby(['ticker']).cumcount()+1
big2['Rank'] = (big2['Rank'] -1) % 40 + 1
#40 Consolidation zone, the lagged values are from the
big2['cons40max'] = np.where(big2['Rank']>10 , big2['nhmax30'],big2['nhmax20'])
big2['cons40min'] = np.where(big2['Rank']>10 , big2['nlmin30'],big2['nlmin20'])
#Calculate the difference between the high and low
big2['cons40diff']=big2['cons40max']-big2['cons40min']
big2['cons40tight']=big2['cons40diff']/big2['Close']
#print big2
   
#big2.to_csv('st31')     
