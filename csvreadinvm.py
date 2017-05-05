import pandas as pd

df = pd.read_csv('hope/London.csv')
df['firstchar'] = df['phaseone_ticker'].astype(str).str[0]
#Identify the ones that have a digit in the first spot
df2=df[df['firstchar'].str.contains('|'.join(searchfor))]
df2['digit flag']=1
df3=df2['digit flag','phaseone_ticker']

print df.dtypes
