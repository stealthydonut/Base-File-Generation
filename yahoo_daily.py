ticker = ['XOM','MSFT']

import urllib
import pandas as pd

#Create a loop which iterates through to generate the csv and add the data to a dataframe

#https://stackoverflow.com/questions/2960772/putting-a-variable-inside-a-string-python

for i in ticker:
    #Develop the text string that can get all the data
    start="http://finance.yahoo.com/d/quotes.csv?s="
    end="&f=j1f6oghps7ns"
    str1 = ''.join([i])
    text2=start+str1+end    
    #Get the data from the yahoo api
    link=text2
    f = urllib.urlopen(link)
    myfile = f.read()
    df = pd.read_csv(f)

