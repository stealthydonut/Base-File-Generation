import pandas as pd
import numpy as np
import quandl as qd
import os
import time
import datetime as dt
import pandas.core.algorithms as algos
import StringIO
from google.cloud import storage
big = pd.DataFrame()

client = storage.Client()
bucket = client.get_bucket('stagingarea')
# Then do other things...
blob = bucket.get_blob('lse_history_stagingarea.csv')
# Define the object
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
bigdata=pd.read_csv(inMemoryFile, low_memory=False)

for i in range(0,165):  
    data=bigdata[bigdata['key_cnt'] >= i] 
    data2=data[data['key_cnt'] < 150 + i]
    data2['value']=i
    data2=data2.sort_values(['ticker','dates'],ascending=True)
    #Determine if position is hitting new highs and new lows within a specified time frame
    #Create the index to determine the number of day  
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
    for i in range(1): 
        test2=data2.groupby('ticker').tail(i)
        big=big.append(test2, ignore_index=True)


#Put the dataset back into storage
bucket2 = client.get_bucket('basefilegeneration')
df_out = pd.DataFrame(big)
df_out.to_csv('lse_history_base.csv', index=False)
blob2 = bucket2.blob('lse_history_base.csv')
blob2.upload_from_filename('lse_history_base.csv')

