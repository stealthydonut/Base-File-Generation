import pandas as pd

df = pd.read_csv('hope/London.csv')
df['firstchar'] = df['phaseone_ticker'].astype(str).str[0]
#Identify the ones that have a digit in the first spot
df2=df[df['firstchar'].str.contains('|'.join(searchfor))]
df2['digit flag']=1
df3=df2[['digit flag','phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker']]
londonpre=pd.merge(df, df3, how='left',  left_on=['phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker'], right_on=['phaseone_id_bb_company','phaseone_id_bb_parent_co','phaseone_ticker'])
london_ticker=londonpre[londonpre['digit flag']!=1.0]
london_ticker.drop_duplicates(cols='phaseone_ticker', take_last=True)
london_ticker_gold=london_ticker.drop_duplicates(subset=['phaseone_ticker'], keep='last')
