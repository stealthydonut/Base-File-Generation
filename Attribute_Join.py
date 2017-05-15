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

#Get the source data from Bloomberg
bucket2 = client.get_bucket('bloomberg')
# Then do other things...
blob = bucket2.get_blob('ln.csv')
# Define the object
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
source=pd.read_csv(inMemoryFile, low_memory=False)


##Begin to join bloomberg file to the historical prices
source2=source[source['phaseone_issuer_name_types']=='Company']
#Eliminate all the null values
source3=source2[source2['phaseone_company_to_parent_relationship'].isnull()]
source3=source3.drop_duplicates(subset=['phaseone_ticker'], keep='last')
source3['quandl']="LSE/"
source3["quandl_ticker"] = source3["quandl"].map(str) + source3["phaseone_ticker"]
#Only keep the details from source
source_gold=source3[['quandl_ticker','phaseone_id_bb_company','phaseone_industry_sector','phaseone_industry_group','phaseone_industry_subgroup']]
#Clean up the data and get rid of whitespaces
source_gold['ticker'] = source_gold['quandl_ticker'].apply(lambda x: x.strip())

#merge the files together
bigdata2=pd.merge(bigdata, source_gold, how='left', left_on=['ticker'], right_on=['ticker'])

#Put the dataset back into storage
bucket2 = client.get_bucket('stagingarea')
df_out = pd.DataFrame(bigdata2)
df_out.to_csv('lse_history_stagingarea_w_attributes.csv', index=False)
blob2 = bucket2.blob('lse_history_stagingarea_w_attributes.csv')
blob2.upload_from_filename('lse_history_stagingarea_w_attributes.csv')

