#!/usr/bin/env python
# coding: utf-8


import json
import requests
from box import Box as box
import time
from tqdm.notebook import tqdm
import pandas as pd
import os
# print()

# TOKEN = os.environ['CT_TOKEN']

# terms = []
# searchTerms = get_declancions(terms)

def split_to_queries(searchTerms, max_length = 910):
    queries = []
    query = ''
    for searchTerm in searchTerms:
        if len(query + searchTerm) > max_length:
            queries.append(query.rstrip(','))
            query = ''  
        query += f'"{searchTerm}",'
    queries.append(query.rstrip(','))
    return queries

def get_query_results_fb(queries, date_from, date_to, list_id = 1567015):
    if os.environ['VERBOSE'] == 'VERBOSE':
        print('get_query_results_fb', queries, date_from, date_to, list_id)
    
    results = []
    TOKEN = os.getenv('CROWDTANGLE_TOKEN')
    # return pd.DataFrame()
    for query in queries:
        res = {"result":{"pagination":{"nextPage":None}}}
        offset = 0

        while 'nextPage' in res["result"]["pagination"]:

            params = dict(
                token = TOKEN,
                startDate = date_from,
                endDate = date_to,
                count = 100,
                listIds = list_id,
                searchTerm = query,
                offset = 100 * offset
            )
            offset += 1
            res = requests.get("https://api.crowdtangle.com/posts", params = params).json()

            results += res["result"]["posts"]
            time.sleep(11)
    
    return pd.DataFrame(results)
