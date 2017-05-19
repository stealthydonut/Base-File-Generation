import pandas as pd
import numpy as np
import quandl as qd
import os
import time
import datetime as dt
import pandas.core.algorithms as algos
import StringIO
qd.ApiConfig.api_key ='BVno6pBYgcEvZJ6uctTr'
bigdata = pd.DataFrame()
badlist = pd.DataFrame()
df1 = pd.DataFrame()

#Get the ticker data from google cloud
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('bloomberg')
# Then do other things...
blob = bucket.get_blob('cn.csv')
# Define the object
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
df=pd.read_csv(inMemoryFile, low_memory=False)

#Begin to isolate only the tickers required in a list
df['firstchar'] = df['phaseone_ticker'].astype(str).str[0]
#Identify the ones that have a digit in the first spot
searchfor=['1','2','3','4','5','6','7','8','9','0']
df2=df[df['firstchar'].str.contains('|'.join(searchfor))]
df2['digit flag']=1
df3=df2[['digit flag','phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker']]
pre=pd.merge(df, df3, how='left',  left_on=['phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker'], right_on=['phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker'])
ticker=pre[pre['digit flag']!=1.0]
ticker_gold=ticker.drop_duplicates(subset=['phaseone_ticker'], keep='last')
ticker_gold['quandl']="YAHOO/TO_"
ticker_gold["quandl_ticker"] = ticker_gold["quandl"].map(str) + ticker_gold["phaseone_ticker"]
ticker_gold2=ticker_gold['quandl_ticker']
#convert to a ticker list
ticker_country=ticker_gold2.values.T.tolist()
#strip out leading and trailing 0's
ticker_country = [x.strip(' ') for x in ticker_country]

#Print the ticker list
for i in ticker_country:    
    try:     
        data = qd.get(i)
        data['ticker']= i
        data['index1'] = data.index
        data['dates'] = [dt.datetime(year=d.year, month=d.month, day=d.day) for d in data['index1']]
        data['year'] = data['index1'].dt.strftime("%Y")
        data['month'] = data['index1'].dt.strftime("%m")
        data['day'] = data['index1'].dt.strftime("%d")
        data['Close']=data['Last Close']
        data['Open']=data['Price']
        data['close_lag1']=data['Close'].shift(1)
        data['changepos']=np.where(data['Close']>data['close_lag1'], 1, 0)
        data['changeneg']=np.where(data['Close']<data['close_lag1'], 1, 0)
        data['changenone']=np.where(data['Close']==data['close_lag1'], 1, 0)
        bigdata = bigdata.append(data, ignore_index=False)
        errortail = datax.tail(1)
        error = error.append(errortail)
    except:
        print i + ' error'
        print dt.datetime.now()
        
   
#Put the dataset back into storage
bucket2 = client.get_bucket('historyprices')
df_out = pd.DataFrame(bigdata)
df_out.to_csv('ca_history.csv', index=False)
blob2 = bucket2.blob('ca_history.csv')
blob2.upload_from_filename('ca_history.csv')
