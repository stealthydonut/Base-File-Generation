import pandas as pd
import numpy as np
import quandl as qd
import os
import time
import datetime as dt
import pandas.core.algorithms as algos
import StringIO
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('historyprices')
# Then do other things...
blob = bucket.get_blob('lse_history.csv')
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
#Begin to build methodology to calculate Probability Runs
#Convert the dataframe to a list so we can use the power of python iteration
#Positive Runs
dfList =bigdata['changepos'].tolist()
#print dfList
c=[]
i=0
for d in dfList:
    if d==0:
        i=0
    else:
        i+=1
    c.append(i)

bigdata['positive_runs'] = pd.Series(c, index=bigdata.index)
#Negative Runs
dfList =bigdata['changeneg'].tolist()
#print dfList
c=[]
i=0
for d in dfList:
    if d==0:
        i=0
    else:
        i+=1
    c.append(i)

bigdata['negative_runs'] = pd.Series(c, index=bigdata.index)
bigdata['counter']=1
################################################################################################################################################
#Build a key that incrementally all the parts of the run together (i.e. run has three components - but will like them to have a master run key)#
################################################################################################################################################
dflist = bigdata['negative_runs'].tolist()
c=[]
i=0
for d in dflist:
    if d==0:
        i+=1
    c.append(i)

bigdata['negative_master_run'] = pd.Series(c, index=bigdata.index)
################################################################################################################################################
#Build a key that incrementally all the parts of the run together (i.e. run has three components - but will like them to have a master run key)#
################################################################################################################################################
dflist = bigdata['positive_runs'].tolist()
c=[]
i=0
for d in dflist:
    if d==0:
        i+=1
    c.append(i)

bigdata['positive_master_run'] = pd.Series(c, index=bigdata.index)
############################################################################
#Build Metrics that can be used to evaluate how sequence runs end and begin#
############################################################################
bigdata['per_change']=1-bigdata['close_lag1']/bigdata['Close']
bigdata['per_change_lag1']=bigdata['per_change'].shift(1)
bigdata['per_change_lag2']=bigdata['per_change'].shift(2)
bigdata['per_change_lag3']=bigdata['per_change'].shift(3)
bigdata['per_change_lag4']=bigdata['per_change'].shift(4)
bigdata['per_change_lag5']=bigdata['per_change'].shift(5)
bigdata['per_change_lag6']=bigdata['per_change'].shift(6)
bigdata['high close per']=(bigdata['High']-bigdata['Close'])/bigdata['Close']
bigdata['low close per']=(bigdata['Close']-bigdata['Low'])/bigdata['Close']
bigdata['open change']=(bigdata['Open']-bigdata['close_lag1'])/bigdata['close_lag1']
#####################################################################################
#Both the negative and positive are using minimum because we are trying to determine#
#the difference between the change and close
bigdata['max run high'] = bigdata.groupby(['ticker','positive_master_run'])['high close per'].cummin()
bigdata['min run low'] = bigdata.groupby(['ticker','negative_master_run'])['low close per'].cummin()
#Construct analytics to understand when prices revert to the mean - objective
#is to understand whether the degree of mean difference has impacts on sequences
bigdata['ma10']=bigdata.groupby('ticker')['Close'].apply(pd.rolling_mean, 10, min_periods=1)
bigdata['ma20']=bigdata.groupby('ticker')['Close'].apply(pd.rolling_mean, 20, min_periods=1)
bigdata['ma40']=bigdata.groupby('ticker')['Close'].apply(pd.rolling_mean, 40, min_periods=1)
bigdata['ma150']=bigdata.groupby('ticker')['Close'].apply(pd.rolling_mean, 150, min_periods=1)
bigdata['ma10hl']=np.where(bigdata['Close']<bigdata['ma10'], 1, 0)
bigdata['ma20hl']=np.where(bigdata['Close']<bigdata['ma20'], 1, 0)
bigdata['ma40hl']=np.where(bigdata['Close']<bigdata['ma40'], 1, 0)
bigdata['ma150hl']=np.where(bigdata['Close']<bigdata['ma150'], 1, 0)
#Determine how different the close was from the average#
bigdata['ma10diffstart']=((bigdata['Close']-bigdata['ma10'])/bigdata['Close'])
bigdata['ma20diffstart']=((bigdata['Close']-bigdata['ma20'])/bigdata['Close'])
bigdata['ma40diffstart']=((bigdata['Close']-bigdata['ma40'])/bigdata['Close'])
bigdata['ma150diffstart']=((bigdata['Close']-bigdata['ma150'])/bigdata['Close'])
############################################
#Build metrics on high, low, close and open#
############################################
bigdata['high_close_diff']=(bigdata['High']-bigdata['Close'])/bigdata['Close']
bigdata['low_close_diff']=(bigdata['Low']-bigdata['Close'])/bigdata['Close']
bigdata['low_open_diff']=(bigdata['Low']-bigdata['Open'])/bigdata['Open']
bigdata['high_open_diff']=(bigdata['High']-bigdata['Open'])/bigdata['Open']
bigdata['high_low_diff']=(bigdata['High']-bigdata['Low'])
###################################################################
#Begin to segment daily price behavior into measurable definitions#
###################################################################
bigdata['down day']=np.where(bigdata['high_open_diff']< 0.001, 1, 0)
bigdata['up day']=np.where(bigdata['low_open_diff']> -0.001, 1, 0)
bigdata['up close']=np.where(bigdata['high_close_diff']< 0.001, 1, 0)
bigdata['down close']=np.where(bigdata['low_close_diff']> -0.001, 1, 0)
bigdata['open_lag1']=bigdata['Open'].shift(1)
bigdata['high_lag1']=bigdata['High'].shift(1)
bigdata['low_lag1']=bigdata['Low'].shift(1)
bigdata['neg1 down day'] = np.where(((bigdata['high_open_diff']< 0.001) & (bigdata['negative_runs']==0)), 1, 0)
bigdata['neg2 down day'] = np.where(((bigdata['high_open_diff']< 0.001) & (bigdata['negative_runs']==1)), 1, 0)
bigdata['neg3 down day'] = np.where(((bigdata['high_open_diff']< 0.001) & (bigdata['negative_runs']==2)), 1, 0)
bigdata['neg4 down day'] = np.where(((bigdata['high_open_diff']< 0.001) & (bigdata['negative_runs']==3)), 1, 0)
bigdata['neg5 down day'] = np.where(((bigdata['high_open_diff']< 0.001) & (bigdata['negative_runs']==4)), 1, 0)
bigdata['neg6 down day'] = np.where(((bigdata['high_open_diff']< 0.001) & (bigdata['negative_runs']==5)), 1, 0)
bigdata['neg1 down close'] = np.where(((bigdata['low_close_diff']> -0.001) & (bigdata['negative_runs']==0)), 1, 0)
bigdata['neg2 down close'] = np.where(((bigdata['low_close_diff']> -0.001) & (bigdata['negative_runs']==1)), 1, 0)
bigdata['neg3 down close'] = np.where(((bigdata['low_close_diff']> -0.001) & (bigdata['negative_runs']==2)), 1, 0)
bigdata['neg4 down close'] = np.where(((bigdata['low_close_diff']> -0.001) & (bigdata['negative_runs']==3)), 1, 0)
bigdata['neg5 down close'] = np.where(((bigdata['low_close_diff']> -0.001) & (bigdata['negative_runs']==4)), 1, 0)
bigdata['neg6 down close'] = np.where(((bigdata['low_close_diff']> -0.001) & (bigdata['negative_runs']==5)), 1, 0)
bigdata['cnt']=1
#########################################################
#Build negative and positive reverse sequence run counts#
#########################################################
bigdata=bigdata.sort_values(['ticker','dates','negative_runs'],ascending=False)
#Identify only the sequences that are actually negative#
negseq=bigdata[bigdata['negative_runs']==1]
negseq2=negseq[['negative_master_run']]
negseq2['index1'] = negseq2.index
bigdata['index1'] = bigdata.index
bigdata2=pd.merge(negseq2, bigdata, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
################################
#Build the reverse run variable#
################################
dfList =bigdata2['negative_runs'].tolist()
#print dfList
c=[]
i=0
for d in dfList:
    if d>0:
        i+=1
    else:
        i=0
    c.append(i)

bigdata2['negative_runs_reverse'] = pd.Series(c, index=bigdata2.index)
#################################################
#Percentage change based on day in sequence run##
#################################################
lastday=bigdata2[bigdata2['negative_runs']==1]
lastday['firstday1perchange']=lastday['per_change']
lastday=lastday[['negative_master_run','firstday1perchange','ticker','dates']]
secondday=bigdata2[bigdata2['negative_runs']==2]
secondday['firstday2perchange']=secondday['per_change']
secondday=secondday[['negative_master_run','firstday2perchange','ticker','dates']]
thirdday=bigdata2[bigdata2['negative_runs']==3]
thirdday['firstday3perchange']=thirdday['per_change']
thirdday=thirdday[['negative_master_run','firstday3perchange','ticker','dates']]
forthday=bigdata2[bigdata2['negative_runs']==4]
forthday['firstday4perchange']=forthday['per_change']
forthday=forthday[['negative_master_run','firstday4perchange','ticker','dates']]
fifthday=bigdata2[bigdata2['negative_runs']==5]
fifthday['firstday5perchange']=fifthday['per_change']
fifthday=fifthday[['negative_master_run','firstday5perchange','ticker','dates']]
  
bigdata2a=pd.merge(bigdata2, lastday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
bigdata2b=pd.merge(bigdata2a, secondday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
bigdata2c=pd.merge(bigdata2b, thirdday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
bigdata2d=pd.merge(bigdata2c, forthday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
bigdata2e=pd.merge(bigdata2d, fifthday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
     
lastday=bigdata2[bigdata2['negative_runs_reverse']==1]
lastday['lastday1perchange']=lastday['per_change']
lastday=lastday[['negative_master_run','lastday1perchange','ticker','dates']]
secondday=bigdata2[bigdata2['negative_runs_reverse']==2]
secondday['lastday2perchange']=secondday['per_change']
secondday=secondday[['negative_master_run','lastday2perchange','ticker','dates']]
thirdday=bigdata2[bigdata2['negative_runs_reverse']==3]
thirdday['lastday3perchange']=thirdday['per_change']
thirdday=thirdday[['negative_master_run','lastday3perchange','ticker','dates']]
forthday=bigdata2[bigdata2['negative_runs_reverse']==4]
forthday['lastday4perchange']=forthday['per_change']
forthday=forthday[['negative_master_run','lastday4perchange','ticker','dates']]
fifthday=bigdata2[bigdata2['negative_runs_reverse']==5]
fifthday['lastday5perchange']=fifthday['per_change']
fifthday=fifthday[['negative_master_run','lastday5perchange','ticker','dates']]
  
bigdata2ax=pd.merge(bigdata2e, lastday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
bigdata2bx=pd.merge(bigdata2ax, secondday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
bigdata2cx=pd.merge(bigdata2bx, thirdday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
bigdata2dx=pd.merge(bigdata2cx, forthday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
bigdata2ex=pd.merge(bigdata2dx, fifthday, how='left', left_on=['negative_master_run','ticker','dates'], right_on=['negative_master_run','ticker','dates'])
    
        
#This generates the values at the sequence run level
     
lastday=bigdata2[bigdata2['negative_runs_reverse']==1]
lastday['last1perchange']=lastday['per_change']
lastday=lastday[['negative_master_run','last1perchange']]
secondday=bigdata2[bigdata2['negative_runs_reverse']==2]
secondday['last2perchange']=secondday['per_change']
secondday=secondday[['negative_master_run','last2perchange']]
thirdday=bigdata2[bigdata2['negative_runs_reverse']==3]
thirdday['last3perchange']=thirdday['per_change']
thirdday=thirdday[['negative_master_run','last3perchange']]
forthday=bigdata2[bigdata2['negative_runs_reverse']==4]
forthday['last4perchange']=forthday['per_change']
forthday=forthday[['negative_master_run','last4perchange']]
fifthday=bigdata2[bigdata2['negative_runs_reverse']==5]
fifthday['last5perchange']=fifthday['per_change']
fifthday=fifthday[['negative_master_run','last5perchange']]
  
bigdata2a=pd.merge(bigdata2ex, lastday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
bigdata2b=pd.merge(bigdata2a, secondday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
bigdata2c=pd.merge(bigdata2b, thirdday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
bigdata2d=pd.merge(bigdata2c, forthday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
bigdata2e=pd.merge(bigdata2d, fifthday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
  
lastday=bigdata2[bigdata2['negative_runs']==1]
lastday['first1perchange']=lastday['per_change']
lastday=lastday[['negative_master_run','first1perchange']]
secondday=bigdata2[bigdata2['negative_runs']==2]
secondday['first2perchange']=secondday['per_change']
secondday=secondday[['negative_master_run','first2perchange']]
thirdday=bigdata2[bigdata2['negative_runs']==3]
thirdday['first3perchange']=thirdday['per_change']
thirdday=thirdday[['negative_master_run','first3perchange']]
forthday=bigdata2[bigdata2['negative_runs']==4]
forthday['first4perchange']=forthday['per_change']
forthday=forthday[['negative_master_run','first4perchange']]
fifthday=bigdata2[bigdata2['negative_runs']==5]
fifthday['first5perchange']=fifthday['per_change']
fifthday=fifthday[['negative_master_run','first5perchange']]
  
bigdata2f=pd.merge(bigdata2e, lastday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
bigdata2g=pd.merge(bigdata2f, secondday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
bigdata2h=pd.merge(bigdata2g, thirdday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
bigdata2i=pd.merge(bigdata2h, forthday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
bigdata2_gold=pd.merge(bigdata2i, fifthday, how='left', left_on=['negative_master_run'], right_on=['negative_master_run'])
 
#Put the dataset back into storage
bucket2 = client.get_bucket('stagingarea')
df_out = pd.DataFrame(bigdata2_gold)
df_out.to_csv('lse_history_stagingarea.csv', index=False)
blob2 = bucket2.blob('lse_history_stagingarea.csv')
blob2.upload_from_filename('lse_history_stagingarea.csv')
