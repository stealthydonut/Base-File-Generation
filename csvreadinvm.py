import pandas as pd
import numpy as np
import quandl as qd
import os
import time
import datetime as dt
import pandas.core.algorithms as algos
qd.ApiConfig.api_key ='BVno6pBYgcEvZJ6uctTr'
bigdata = pd.DataFrame()
badlist = pd.DataFrame()
df1 = pd.DataFrame()

df = pd.read_csv('hope/London.csv')
df['firstchar'] = df['phaseone_ticker'].astype(str).str[0]
#Identify the ones that have a digit in the first spot
searchfor=['1','2','3','4','5','6','7','8','9','0']
df2=df[df['firstchar'].str.contains('|'.join(searchfor))]
df2['digit flag']=1
df3=df2[['digit flag','phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker']]
londonpre=pd.merge(df, df3, how='left',  left_on=['phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker'], right_on=['phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker'])
london_ticker=londonpre[londonpre['digit flag']!=1.0]
london_ticker.drop_duplicates(cols='phaseone_ticker', take_last=True)
london_ticker_gold=london_ticker.drop_duplicates(subset=['phaseone_ticker'], keep='last')
london_ticker_gold['quandl']="LSE/"
london_ticker_gold["quandl_ticker"] = london_ticker_gold["quandl"].map(str) + london_ticker_gold["phaseone_ticker"]
london_ticker_gold2=london_ticker_gold['quandl_ticker']
#convert to a ticker list
lse_ticker=london_ticker_gold2.values.T.tolist()
#strip out leading and trailing 0's
lse_ticker = [x.strip(' ') for x in lse_ticker]


#Print the ticker list
for i in lse_ticker:    
    try:     
        data = qd.get(i[0])
        data['ticker']= i[0]
        data['index1'] = data.index
        data['dates'] = [dt.datetime(year=d.year, month=d.month, day=d.day) for d in data['index1']]
        data['year'] = data['index1'].dt.strftime("%Y")
        data['month'] = data['index1'].dt.strftime("%m")
        data['day'] = data['index1'].dt.strftime("%d")
        data['close_lag1']=data['Close'].shift(1)
        data['changepos']=np.where(data['Close']>data['close_lag1'], 1, 0)
        data['changeneg']=np.where(data['Close']<data['close_lag1'], 1, 0)
  
        bigdata = bigdata.append(data, ignore_index=True)
        errortail = data.tail(1)
        error = error.append(errortail)
        print dt.datetime.now()
    except:
        print i[0] + ' error'
        print dt.datetime.now()
