#!/usr/bin/env python
# coding: utf-8

# In[1]:

from searchtweets import load_credentials,gen_request_parameters,collect_results,result_stream,utils
# from secrets_ar import *
import pandas as pd
import glob
import os
import seaborn as sns
import langid,re
import collections
import matplotlib.pyplot as plt
from googletrans import Translator

def parseOperators(df):
    
    df[0] = df[0].apply(lambda x:'('+x+')')
    # Put brackets around each
    
    df[0] = df[0].apply(lambda x:re.sub(' OR ',') OR (',x))
    # Break up ORs into separate clauses
    
    df[0] = df[0].apply(lambda x:re.sub(' AND ',' ',x))
    # Replace AND operator with a space
    
    return df

def cleanText(s):
    s = re.sub(r'[/\\\-]',' ',s)
    return re.sub(r'[^\w\s]','',s)

def getLang(t):
    return langid.classify(t)[0]

def makeSimpleQuery():
    '''
    Makes a simple query to be input into TW search API
    'NK', 'Armenia', 'Azerbaijan' in either language
    Returns a list of queries @qs and language prefix @prefix
    Note: Currently hardcoded. If amended query must be 
    less than 1024 characters or else it must be split
    '''

    print('Making simple query...')
    
    prefix = ''

    while not prefix in ['az','hy']:

        prefix = input('Which query (AZ/hy)?')

        if prefix == '':
            prefix = 'az'


    print(prefix+' chosen')

    if prefix == 'hy':
        qs = ['Լեռնային Ղարաբաղ OR Ադրբեջան OR Ադրբեջանցի']
        prefix = 'hy'
        #HY
    else:
        qs = ['Dağlıq Qarabağ OR Erməni OR Ermənistan']
        prefix = 'az'
        #AZ
        
    return qs,prefix

def removeNoisyTerms(df,noisyTerms = ['veteran','truce']):
    '''
    Removes a set of noisy terms from DataFrame with
    all declined keywords
    Returns @df
    '''

    removeNoisy = input('Remove noisy terms? ({:s}) (Y/n)'.format(','.join(noisyTerms)))

    if removeNoisy == 'n':
        removeNoisy = False
        print('Not removing')
    else:
        removeNoisy == True
        print('Removing')

        df = df[~df[0].isin(noisyTerms)]
        
    return df

def splitQueriesSimple(keywords, max_query_lenght = 400, additional_query_parameters = ''):
    '''
    Simpler verstion to generate the query strings from list of a keywords
    :param keywords: list[string] list of keywords
    :param max_query_lenght: int the length of generated query strings, 
        depending on account type it might be 400 or 1000 
    :additional_query_parameters 

    :return :list[string] of generated query strings
    '''
    queries = []
    query = keywords[0]

    for keyword in keywords[1:]:
        tmp_query = '{} OR "{}"'.format(query, keyword)
        if len(tmp_query + additional_query_parameters) > max_query_lenght:
            queries.append(f'{tmp_query}  {additional_query_parameters}')
            query = f'"{keyword}"'
            continue
        query = tmp_query

    queries.append(f'{tmp_query}  {additional_query_parameters}')

    return queries

def splitQueries(declensionsDf,prefix,writeToFile = True):
    '''
    Function to take a DataFrame of keywords and
    combine with OR operators to make a series of
    queries under 1024 characters. Optionally write 
    the queries to a series of files
    '''
    
    n = 0
    lastN = 0
    nFile = 0
    tempQ = ''
    qs = []
    
    print('Splitting queries')
    
    if writeToFile:
        path = input('Enter path stem (query_{:s}[_<n>.csv])'.format(prefix))
        
        if path == '':
            path = 'query_{:s}'.format(prefix)
       
    
    cleanPath = 'n'
        
    cleanPath = input('Clean existing query files? (y/N)').lower()

    if cleanPath in ['','y']:
        cleanPath = True
    else:
        cleanPath = False
            
    if cleanPath:
        print('Removing {:s}*'.format(path))
        
        for file in glob.glob('{:s}*'.format(path)):
            os.remove(file)


    print('Shape:',declensionsDf.shape[0])
    
    declensionsDf[0] = parseOperators(declensionsDf)
            
    while n < declensionsDf.shape[0]:

        tempQ = ' OR '.join(declensionsDf[0].values[lastN:n])

        if len(tempQ) > 1024:
            qs.append(' OR '.join(declensionsDf[0].values[lastN:n-1]))

            if writeToFile:
                print('Writing to file : ' + '{:s}_{:d}.csv'.format(path,nFile))
                
                queryFileName = '{:s}_{:d}.csv'.format(path,nFile)
                
                with open(queryFileName,'w') as outFile:
                    outFile.writelines(qs[-1])

            print('\tLength written {:d}'.format(len(qs[-1])))
            print()

            lastN = n
            n-=1
            nFile+=1

        n+=1
        
    if nFile == 0:    
    # In case all keywords fit in one 1024 query string
        qs.append(' OR '.join(declensionsDf[0].values[lastN:n-1]))

        if writeToFile:

            print('Writing to file : ' + '{:s}_{:d}.csv'.format(path,nFile))

            queryFileName = '{:s}_{:d}.csv'.format(path,nFile)

            with open(queryFileName,'w') as outFile:
                outFile.writelines(qs[-1])

            print('\tLength written {:d}'.format(len(qs[-1])))
            print()
        
    return qs

def makeComplexQuery(denoise = False):
    '''
    Function to create a query for input into
    Twitter search API based on keywords read
    from files.
    Returns list of query strings @qs and 
    language prefix @prefix
    '''
    
    print('Making complex query...')

    prefix = ''

    while not prefix in ['az','hy']:

        prefix = input('Which query (AZ/hy)?')

        if prefix == '':
            prefix = 'az'

        print(prefix+' chosen')
        
    print('Getting list of declined keywords...')

    fileName = ''

    fileName = input('Enter file path for keywords(default: {:s}_declensions.csv)'.format(prefix))

    if fileName == '':
        fileName = '{:s}_declensions.csv'.format(prefix)
        
    print('Reading declined keywords file...')

    declensionsDf = pd.read_csv(fileName,header=None,sep = '\t')

    declensionsDf.iloc[:,0] = declensionsDf.iloc[:,0]

    print('Got {:d} keywords'.format(declensionsDf.shape[0]))
    
    if denoise:
        declensionsDf = removeNoisyTerms(declensionsDf)
        
    qs = splitQueries(declensionsDf,prefix)
    
    return qs,prefix

def getTokens(df,drop = False):
    '''
    Convenience function to deal with the paging
    information added into results returned
    Returns @tokenDf and @df, with tokens and tweets
    respectively
    '''
    
    if 'newest_id' in df.columns:
        tokenDf = df[~pd.isna(df['newest_id'])]

        if drop:
            df = df[pd.isna(df['newest_id'])]

        return tokenDf,df
    else:
        return pd.DataFrame(),df

def executeQueries(qs,prefix,startTime,search_args,period = '1 days',nResults = 100000,verbose = True, results_per_call= 100):
    '''
    Main routine to execute requests against search API
    for each query string. Some logic required to make sure
    each query backfills desired time period.
    ---------------------------------
    Requires 
    @qs - list of query strings
    @prefix - language codes
    @startTime - datetime of latest date to grab
    @period - time to backfill
    @search_args - credentials object for API
    Returns a list of DataFrames @dfs
    '''
        
    dfs = [pd.DataFrame()]*len(qs)
    # Make one empty dataframe for each query
    # We will append to each one

    #nResults = 10000

    for n,q in enumerate(qs):
        print('Query {:d} of {:d}...'.format(n,len(qs)))

        endTime = startTime + pd.to_timedelta(period)

        query = gen_request_parameters(q, False, results_per_call=results_per_call,tweet_fields='text,author_id,id,created_at',                                   start_time=startTime.isoformat()[0:10],end_time=endTime.isoformat()[0:10])

        results = collect_results(query,max_tweets=nResults,result_stream_args=search_args)
        # Grab first batch of tweets to see how close to backfilling we get
    
        print('Grabbing first tweets')

        if len(results) > 0:
            # Check there is at least one match

            tweets = results[:-1]
            metadata = results[-1]

            df = pd.DataFrame(data = tweets)
            df.set_index(pd.to_datetime(df['created_at']),inplace=True)
            
            tokenDf,df = getTokens(df)
            # Get rid of the tokens for now

            if verbose:
                print('Got {:d} tweets'.format(df.shape[0]))
                
            dfs[n] = dfs[n].append(df)
            # Add the new tweets to the array

            if verbose:
                print('Takes us to',df.index[-1].isoformat()[0:-6])

            breakOut = False
            startTimeOffset = pd.to_timedelta('0 days')
            # We need this flag to break the while loop
            # for when the day ranges shift 

            while df.index[-1] > startTime:
            # Keep grabbing tweets for this query 
            # Until entire date range is backfilled
                print(df.index[-1])
                print(startTime)

                endTime = df.index[-1]

                if (endTime - startTime).days == 0:
                    startTimeOffset = pd.to_timedelta('1 hours')
                    # Nudge the start date back by an hour
                    # To make sure that start is always before end
                    # Or API returns error

                if verbose:
                    print('We need more tweets to look further back (to {:s})'.format(startTime.isoformat()[0:10]))
                    print('Querying with:')
                    print('startTime',(startTime - startTimeOffset).isoformat()[0:19])
                    print('endTime',endTime.isoformat()[0:19])

                query = gen_request_parameters(q, results_per_call=results_per_call,tweet_fields='text,author_id,id,created_at',                                   start_time=(startTime - startTimeOffset).isoformat()[0:10],                                               end_time=endTime.to_pydatetime().strftime("%Y-%m-%d %H:%M"))

                results = collect_results(query,max_tweets=nResults,result_stream_args=search_args)
                # Grab 1k tweets first to see how far it goes

                
                if len(results) > 0:
                
                    tweets = results[:-1]
                    metadata = results[-1]


                    df = pd.DataFrame(data = tweets)
                    df.set_index(pd.to_datetime(df['created_at']),inplace=True)

                    tokenDf,df = getTokens(df,drop = True)
                    # Get rid of the tokens for now

                    dfs[n] = dfs[n].append(df)

                    if verbose:
                        print('Takes us to',dfs[n].index[-1].isoformat())
                        print('{:d} tweets so far'.format(dfs[n].shape[0]))
                        print()
                else:
                    print('No results....')
                    dfs[n] = dfs[n].append(pd.DataFrame())
                    breakOut = True

                if breakOut:
                    print('Breaking out...')
                    break

            print('Now we are done')
            print('Got {:d} tweets in total'.format(dfs[n].shape[0]))
            print('Between:')
            print(dfs[n].index[0])
            print(dfs[n].index[-1])
            print('+++++++\n')

        else:
            print('No results...\n+++++++\n')
            dfs.append(pd.DataFrame())
    return dfs

def countTerms(text,stopWords = None):
    '''
    Convenience function to count terms in
    an iterable of text (pandas series, list etc)
    Returns @c counter object
    '''
    
    c = collections.Counter()
    
    text = text.astype(str)
    
    text.apply(lambda x:c.update(x.lower().split()))
    
    if stopWords:
        for sw in stopWords:
            del c[sw]
    
    return c

def writeData(dfs,prefix):
    '''
    Write dataframes with results to file
    '''
    
    stem = input('Enter data file stem (data_{:s}[_<n>.csv])'.format(prefix))
    
    if stem == '':
        stem = 'data_{:s}_'.format(prefix)
    
    for n,df in enumerate(dfs):
        
        fileName = '{:s}{:d}.csv'.format(stem,n)
        df.to_csv(fileName)
        #print('Print to',fileName)
    
def getMatchingKeywords(t,qs):
    '''
    Returns a list of keywords that match a string
    '''
    
    matches = []
    
    tokens = t.lower().split()
    
    for q in qs:
        for kw in q.split(' OR '):
            if kw in tokens:
                #print('MATCHED',kw)
                matches.append(kw)
    return matches

def queryToList(q):
    '''
    Convenience function to split query string back 
    up into keywords
    '''
    return q.split(' OR ')