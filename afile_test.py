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
bucket = client.get_bucket('stagingarea')
# Then do other things...
blob = bucket.get_blob('lse_history_stagingarea_w_attributes.csv')
# Define the object
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
bigdata2_gold=pd.read_csv(inMemoryFile, low_memory=False)

bigdata2_gold['ma150rank']=pd.qcut(bigdata2_gold['ma150diffstart'], 30, labels=False)
bigdata2_gold['last1perchangerank']=pd.qcut(bigdata2_gold['last1perchange'], 10, labels=False)
bigdata2_gold['last2perchangerank']=pd.qcut(bigdata2_gold['last2perchange'], 10, labels=False)
bigdata2_gold['first1perchangerank']=pd.qcut(bigdata2_gold['first1perchange'], 10, labels=False)
bigdata2_gold['first2perchangerank']=pd.qcut(bigdata2_gold['first2perchange'], 10, labels=False)
#####################
#Build rank variable#
#####################
rankvar = pd.DataFrame()
rankvar= bigdata2_gold[bigdata2_gold['negative_runs']==1]
rankvar['first_ma150rank'] = rankvar['ma150rank']
rankvar=rankvar[['ticker','negative_master_run','first_ma150rank']]
bigdata3=pd.merge(bigdata2_gold, rankvar, how='left', left_on=['ticker','negative_master_run'], right_on=['ticker','negative_master_run'])
rankvar = pd.DataFrame()
rankvar= bigdata3[bigdata3['negative_runs_reverse']==1]
rankvar['last_ma150rank'] = rankvar['ma150rank']
rankvar=rankvar[['ticker','negative_master_run','last_ma150rank']]
bigdata3x=pd.merge(bigdata3, rankvar, how='left', left_on=['ticker','negative_master_run'], right_on=['ticker','negative_master_run'])
#Aggregate everything at the ticker level and then continue aggregation depending on analysis
downdayanalysis= bigdata3x.groupby(['ticker','negative_master_run','first_ma150rank','first1perchangerank','first2perchangerank','last1perchangerank','last2perchangerank'], as_index=False)['cnt','neg1 down day','neg2 down day','neg3 down day','neg4 down day','neg5 down day','neg6 down day',\
'neg1 down close','neg2 down close','neg3 down close','neg4 down close','neg5 down close','neg6 down close','firstday1perchange','firstday2perchange','firstday3perchange','firstday4perchange','lastday5perchange','lastday1perchange','lastday2perchange','lastday3perchange','lastday4perchange','lastday5perchange'].sum()
######################################################
#Develop metrics on the negative master runs by ticker
######################################################
downdayanalysis['two start downday']=downdayanalysis['neg1 down day']+downdayanalysis['neg2 down day']
downdayanalysis['two start downclose']=downdayanalysis['neg1 down close']+downdayanalysis['neg2 down close']
downdayanalysis['three start downday']=downdayanalysis['neg1 down day']+downdayanalysis['neg2 down day']+downdayanalysis['neg3 down day']
downdayanalysis['three start downclose']=downdayanalysis['neg1 down close']+downdayanalysis['neg2 down close']+downdayanalysis['neg3 down close']
downdayanalysis['four start downday']=downdayanalysis['neg1 down day']+downdayanalysis['neg2 down day']+downdayanalysis['neg3 down day']+downdayanalysis['neg4 down day']
downdayanalysis['four start downclose']=downdayanalysis['neg1 down close']+downdayanalysis['neg2 down close']+downdayanalysis['neg3 down close']+downdayanalysis['neg4 down close']+downdayanalysis['neg5 down day']
downdayanalysis['five start downday']=downdayanalysis['neg1 down day']+downdayanalysis['neg2 down day']+downdayanalysis['neg3 down day']+downdayanalysis['neg4 down day']+downdayanalysis['neg5 down close']
downdayanalysis['five start downclose']=downdayanalysis['neg1 down close']+downdayanalysis['neg2 down close']+downdayanalysis['neg3 down close']+downdayanalysis['neg4 down close']
downdayanalysis['six start downday']=downdayanalysis['neg1 down day']+downdayanalysis['neg2 down day']+downdayanalysis['neg3 down day']+downdayanalysis['neg4 down day']+downdayanalysis['neg5 down day']+downdayanalysis['neg6 down day']
downdayanalysis['six start downclose']=downdayanalysis['neg1 down close']+downdayanalysis['neg2 down close']+downdayanalysis['neg3 down close']+downdayanalysis['neg4 down close']+downdayanalysis['neg5 down close']+downdayanalysis['neg6 down close']
downdayanalysis['two start downday fl'] = np.where(downdayanalysis['two start downday']==2, 1, 0)
downdayanalysis['three start downday fl'] = np.where(downdayanalysis['three start downday']==3, 1, 0)
downdayanalysis['four start downday fl'] = np.where(downdayanalysis['four start downday']==4, 1, 0)
downdayanalysis['five start downday fl'] = np.where(downdayanalysis['five start downday']==5, 1, 0)
downdayanalysis['six start downday fl'] = np.where(downdayanalysis['six start downday']==6, 1, 0)
downdayanalysis['run count']=downdayanalysis['cnt']
del downdayanalysis['cnt']
downdayanalysis['cntofruns']=1

#Group the data into a form that can be analyzed
afile=downdayanalysis[['negative_master_run','first_ma150rank','first1perchangerank','first2perchangerank','last1perchangerank','last2perchangerank','run count','cntofruns','neg1 down day','neg2 down day','neg3 down day','neg4 down day','neg5 down day','neg6 down day',\
'neg1 down close','neg2 down close','neg3 down close','neg4 down close','neg5 down close','neg6 down close','two start downday fl','three start downday fl']]

afile['seq cnt']=afile[afile['run count']-1]

#Put the dataset back into storage
bucket2 = client.get_bucket('analyticalfile')
df_out = pd.DataFrame(afile)
df_out.to_csv('afile.csv', index=False)
blob2 = bucket2.blob('afile.csv')
blob2.upload_from_filename('afile.csv') 






downdayanalysisall= afile.groupby(['run count','first_ma150rank'], as_index=False)['cntofruns','neg1 down day','neg2 down day','neg3 down day','neg4 down day','neg5 down day','neg6 down day',\
'neg1 down close','neg2 down close','neg3 down close','neg4 down close','neg5 down close','neg6 down close'].sum()
downdayanalysisall['first_ma150rank']=downdayanalysisall['first_ma150rank'].astype(str)
downdayanalysisall['var']= downdayanalysisall['first_ma150rank']
ma150 = downdayanalysisall.pivot(index='var', columns='run count', values='cntofruns')
ma150.columns = ["cnt_" + str(cn).split('.')[0] for cn in ma150.columns]
ma150.reset_index()
ma150['index1'] = ma150.index
ma150['first_ma150rank']=ma150['index1'].str[0:2]
ma150['tablekey']='ma150'
ma150['key1']=ma150['first_ma150rank']
ma150['keynum']=1

#Put the dataset back into storage
bucket2 = client.get_bucket('analyticalfile')
df_out = pd.DataFrame(ma150)
df_out.to_csv('ma150.csv', index=False)
blob2 = bucket2.blob('ma150.csv')
blob2.upload_from_filename('ma150.csv') 
  
