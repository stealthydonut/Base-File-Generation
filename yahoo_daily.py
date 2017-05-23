import urllib
import pandas as pd
import StringIO
import datetime
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO as stio
else:
    from io import StringIO as stio

#Get the data from google cloud storage
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('gdxjtrade')
# Then do other things...
blob = bucket.get_blob('GDXJ_TICKERS.csv')
content = blob.download_as_string()
#Because the pandas dataframe can only read from buffers or files, we need to take the string and put it into a buffer
inMemoryFile = StringIO.StringIO()
inMemoryFile.write(content)
#When you buffer, the "cursor" is at the end, and when you read it, the starting position is at the end and it will not pick up anything
inMemoryFile.seek(0)
#Note - anytime you read from a buffer you need to seek so it starts at the beginning
#The low memory false exists because there was a lot of data
df=pd.read_csv(inMemoryFile, low_memory=False)
gdxj=df['Ticker']
gdxj_ticker=gdxj.values.T.tolist()
#strip out leading and trailing 0's
gdxj_ticker = [x.strip(' ') for x in gdxj_ticker]

#Create a loop which iterates through to generate the csv and add the data to a dataframe

#https://stackoverflow.com/questions/2960772/putting-a-variable-inside-a-string-python

myfile = ''
for i in gdxj_ticker:
    #Develop the text string that can get all the data
    start="http://finance.yahoo.com/d/quotes.csv?s="
    #date,Float Shares,Day's Low,Day's High,Open,Previous Close,Volume,Name,Ticker
    end="&f=d1f6ghopvns"
    str1 = ''.join([i])
    text2=start+str1+end    
    #Get the data from the yahoo api
    link=text2
    f = urllib.urlopen(link)
    myfile += f.readline()

TESTDATA=stio(myfile)

daily_prices = pd.read_csv(TESTDATA, sep=",")

#add a time stamp to the file name
year = datetime.date.today().year
month = datetime.date.today().month
day = datetime.date.today().day
stamp=str(year)+'_'+str(month)+'_'+str(day)
name='daily_prices'
end='.csv'
fix=name+stamp+end

#Put the dataset back into storage
bucket2 = client.get_bucket('gdxjtrade')
df_out = pd.DataFrame(daily_prices)
df_out.to_csv(fix, index=False)
blob2 = bucket2.blob(fix)
blob2.upload_from_filename(fix)
