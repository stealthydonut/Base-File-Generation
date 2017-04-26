#print bigdata3.dtypes
#bigdata3x=bigdata3.drop_duplicates(subset=['negative_master_run','dates'], keep='last')
#print len(bigdata3x.index)
#print len(rankvar.index)
 
#Begin to Rank the variables#
bigdata2_gold['ma150rank']=pd.qcut(bigdata2_gold['ma150diffstart'], 30, labels=False)
bigdata2_gold['last1perchangerank']=pd.qcut(bigdata2_gold['last1perchange'], 10, labels=False)
bigdata2_gold['last2perchangerank']=pd.qcut(bigdata2_gold['last2perchange'], 10, labels=False)
bigdata2_gold['first1perchangerank']=pd.qcut(bigdata2_gold['first1perchange'], 10, labels=False)
bigdata2_gold['first2perchangerank']=pd.qcut(bigdata2_gold['first2perchange'], 10, labels=False)
 
#Begin to rank the data based on number of different BINS
#bins = algos.quantile(np.unique(bigdata['open change']), np.linspace(0, 1, 11))
#bigdata['open change rank']= pd.tools.tile._bins_to_cuts(bigdata['open change'], bins, include_lowest=True)
#bins = algos.quantile(np.unique(bigdata['high close per']), np.linspace(0, 1, 11))
#bigdata['high close per rank']= pd.tools.tile._bins_to_cuts(bigdata['high close per'], bins, include_lowest=True)
#bins = algos.quantile(np.unique(bigdata['low close per']), np.linspace(0, 1, 11))
#bigdata['low close per rank']= pd.tools.tile._bins_to_cuts(bigdata['low close per'], bins, include_lowest=True)
#bigdata3.to_csv('C:\\Users/davking/Documents/Python Scripts/test')
#bigdata3['high_close_diff_rank'] = pd.qcut(bigdata3['high_close_diff'], 10, labels=False)
#bigdata3['low_close_diff_rank'] = pd.qcut(bigdata3['low_close_diff'], 10, labels=False)
#bigdata3['high_low_diff_rank'] = pd.qcut(bigdata3['high_low_diff'], 10, labels=False)
#bigdata3['low_open_diff_rank'] = pd.qcut(bigdata3['low_open_diff'], 10, labels=False)
#bigdata3['high_open_diff_rank'] = pd.qcut(bigdata3['high_open_diff'], 10, labels=False)
#bigdata3['ma150rank']=pd.qcut(bigdata3['ma150diffstart'], 30, labels=False)
 
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
 
#print bigdata3x.dtypes
 
#Build
 
#
#print bigdata3x
#print bigdata2.dtypes
#print rankvar.dtypes
 
#print bigdata3x[['last1perchange','Close','dates']]
 
 
#Aggregate everything at the ticker level and then continue aggregation depending on analysis
downdayanalysis= bigdata3x.groupby(['ticker','negative_master_run','first_ma150rank','first1perchangerank','first2perchangerank','last1perchangerank','last2perchangerank'], as_index=False)['cnt','neg1 down day','neg2 down day','neg3 down day','neg4 down day','neg5 down day','neg6 down day',\
'neg1 down close','neg2 down close','neg3 down close','neg4 down close','neg5 down close','neg6 down close','firstday1perchange','firstday2perchange','firstday3perchange','firstday4perchange','lastday5perchange','lastday1perchange','lastday2perchange','lastday3perchange','lastday4perchange','lastday5perchange'].sum()
 
 
 
#Develop metrics on the negative master runs by ticker
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
 
afile=downdayanalysis[['negative_master_run','first_ma150rank','first1perchangerank','first2perchangerank','last1perchangerank','last2perchangerank','run count','cntofruns','neg1 down day','neg2 down day','neg3 down day','neg4 down day','neg5 down day','neg6 down day',\
'neg1 down close','neg2 down close','neg3 down close','neg4 down close','neg5 down close','neg6 down close','two start downday fl','three start downday fl']]
 
 
print afile.dtypes
##############################################################################################
#Aggregate everything at the ticker level and then continue aggregation depending on analysis#
##############################################################################################
 
#One key - ma150
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
 
 
#Three Keys ma150, 2dst, 3dst
downdayanalysisall= afile.groupby(['run count','first_ma150rank','two start downday fl','three start downday fl'], as_index=False)['cntofruns'].sum()
downdayanalysisall['first_ma150rank']=downdayanalysisall['first_ma150rank'].astype(str)
downdayanalysisall['two start downdaystring']=downdayanalysisall['two start downday fl'].astype(str)
downdayanalysisall['three start downdaystring']=downdayanalysisall['three start downday fl'].astype(str)
downdayanalysisall['var']= downdayanalysisall['first_ma150rank'] + ' ' + downdayanalysisall['two start downdaystring'] + ' ' + downdayanalysisall['three start downdaystring']
downdayma150 = downdayanalysisall.pivot(index='var', columns='run count', values='cntofruns')
downdayma150.columns = ["cnt_" + str(cn).split('.')[0] for cn in downdayma150.columns]
downdayma150.reset_index()
downdayma150['index1'] = downdayma150.index
downdayma150['first_ma150rank']=downdayma150['index1'].str[0:2]
downdayma150['two start downday fl']=downdayma150['index1'].str[2:4]
downdayma150['three start downday fl']=downdayma150['index1'].str[4:9]
downdayma150['tablekey']='ma150_2stdd_3stdd'
downdayma150['key1']=downdayma150['first_ma150rank']
downdayma150['key2']=downdayma150['two start downday fl']
downdayma150['key3']=downdayma150['three start downday fl']
downdayma150['keynum']=3
 
#Two Keys ma150, pcfirstday
downdayanalysisall= afile.groupby(['run count','first_ma150rank','first1perchangerank'], as_index=False)['cntofruns'].sum()
downdayanalysisall['first_ma150rank']=downdayanalysisall['first_ma150rank'].astype(str)
downdayanalysisall['first1perchangerankstring']=downdayanalysisall['first1perchangerank'].astype(str)
downdayanalysisall['var']= downdayanalysisall['first_ma150rank'] + ' ' + downdayanalysisall['first1perchangerankstring']
ma150day1 = downdayanalysisall.pivot(index='var', columns='run count', values='cntofruns')
ma150day1.columns = ["cnt_" + str(cn).split('.')[0] for cn in ma150day1.columns]
ma150day1.reset_index()
ma150day1['index1'] = ma150day1.index
ma150day1['first_ma150rank']=ma150day1['index1'].str[0:2]
ma150day1['first1perchangerank']=ma150day1['index1'].str[2:4]
ma150day1['tablekey']='ma150_pcd1'
ma150day1['key1']=ma150day1['first_ma150rank']
ma150day1['key2']=ma150day1['first1perchangerank']
ma150day1['keynum']=2
 
#Two Keys ma150, pclastday
downdayanalysisall= afile.groupby(['run count','first_ma150rank','last1perchangerank'], as_index=False)['cntofruns'].sum()
downdayanalysisall['first_ma150rank']=downdayanalysisall['first_ma150rank'].astype(str)
downdayanalysisall['last1perchangerankstring']=downdayanalysisall['last1perchangerank'].astype(str)
downdayanalysisall['var']= downdayanalysisall['first_ma150rank'] + ' ' + downdayanalysisall['last1perchangerankstring']
ma150day1last = downdayanalysisall.pivot(index='var', columns='run count', values='cntofruns')
ma150day1last.columns = ["cnt_" + str(cn).split('.')[0] for cn in ma150day1last.columns]
ma150day1last.reset_index()
ma150day1last['index1'] = ma150day1last.index
ma150day1last['first_ma150rank']=ma150day1last['index1'].str[0:2]
ma150day1last['last1perchangerank']=ma150day1last['index1'].str[2:4]
ma150day1last['tablekey']='ma150_pcd1'
ma150day1last['key1']=ma150day1last['first_ma150rank']
ma150day1last['key2']=ma150day1last['last1perchangerank']
ma150day1last['keynum']=2
 
#Three Keys ma150, pcfirstday, pclastday
downdayanalysisall= afile.groupby(['run count','first_ma150rank','first1perchangerank','last1perchangerank'], as_index=False)['cntofruns'].sum()
downdayanalysisall['first_ma150rank']=downdayanalysisall['first_ma150rank'].astype(str)
downdayanalysisall['first1perchangerankstring']=downdayanalysisall['first1perchangerank'].astype(str)
downdayanalysisall['last1perchangerankstring']=downdayanalysisall['last1perchangerank'].astype(str)
downdayanalysisall['var']= downdayanalysisall['first_ma150rank'] + ' ' + downdayanalysisall['first1perchangerankstring'] + ' ' + downdayanalysisall['last1perchangerankstring']
ma150day1last1first = downdayanalysisall.pivot(index='var', columns='run count', values='cntofruns')
ma150day1last1first .columns = ["cnt_" + str(cn).split('.')[0] for cn in ma150day1last1first .columns]
ma150day1last1first .reset_index()
ma150day1last1first ['index1'] = ma150day1last1first .index
ma150day1last1first ['first_ma150rank']=ma150day1last1first ['index1'].str[0:2]
ma150day1last1first ['first1perchangerank']=ma150day1last1first ['index1'].str[2:4]
ma150day1last1first ['last1perchangerank']=ma150day1last1first ['index1'].str[4:8]
ma150day1last1first ['tablekey']='ma150_pcd1_pcdl'
ma150day1last1first ['key1']=ma150day1last1first ['first_ma150rank']
ma150day1last1first ['key2']=ma150day1last1first ['first1perchangerank']
ma150day1last1first ['key3']=ma150day1last1first ['last1perchangerank']
ma150day1last1first ['keynum']=3
 
 
 
 
 
 
 
 
#Set the datasets on top of one another
file1 = ma150day1last.append(ma150day1last1first, ignore_index=True)
file2 = file1.append(ma150day1, ignore_index=True)
file3 = file2.append(downdayma150, ignore_index=True)
Probability_File = file3.append(ma150, ignore_index=True)
 
 
 
################
#Global Metrics#
################
 
Probability_File['prob4'] = Probability_File['cnt_4']/Probability_File['cnt_3']
Probability_File['prob5'] = Probability_File['cnt_5']/Probability_File['cnt_4']
Probability_File['prob6'] = Probability_File['cnt_6']/Probability_File['cnt_5']
Probability_File['prob7'] = Probability_File['cnt_7']/Probability_File['cnt_6']
Probability_File['prob8'] = Probability_File['cnt_8']/Probability_File['cnt_7']
Probability_File['prob9'] = Probability_File['cnt_9']/Probability_File['cnt_8']
Probability_File['prob10'] = Probability_File['cnt_10']/Probability_File['cnt_9']
Probability_File['prob11'] = Probability_File['cnt_11']/Probability_File['cnt_10']
Probability_File['prob12'] = Probability_File['cnt_12']/Probability_File['cnt_11']
   
   
print downdayma150.dtypes
Probability_File.to_csv('C:\\Users/davking/Documents/Python Scripts/xx2')
