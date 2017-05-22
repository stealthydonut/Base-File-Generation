import urllib

#Create a loop which iterates through to generate the csv and add the data to a dataframe

#https://stackoverflow.com/questions/2960772/putting-a-variable-inside-a-string-python

link = "http://finance.yahoo.com/d/quotes.csv?s=XOM+BBDb.TO+JNJ+MSFT&f=j1f6oghps7ns"
f = urllib.urlopen(link)
myfile = f.read()
print myfile
