import urllib
import pandas as pd
import StringIO
import sys

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

for i in gdxj_ticker:
    #Develop the text string that can get all the data
    start="http://finance.yahoo.com/d/quotes.csv?s="
    end="&f=j1f6oghps7ns"
    str1 = ''.join([i])
    text2=start+str1+end    
    #Get the data from the yahoo api
    link=text2
    f = urllib.urlopen(link)
    myfile += f.read()

TESTDATA=StringIO(myfile)

daily_prices = pd.read_csv(TESTDATA, sep=",")


#Put the dataset back into storage
bucket2 = client.get_bucket('gdxjtrade')
df_out = pd.DataFrame(daily_prices)
df_out.to_csv('daily_prices.csv', index=False)
blob2 = bucket2.blob('daily_prices.csv')
blob2.upload_from_filename('daily_prices.csv')
