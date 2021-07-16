#!/usr/bin/env python
# coding: utf-8


import json
import requests
from box import Box as box
import time
from tqdm.notebook import tqdm
import pandas as pd
import os
print()

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

def get_query_results(queries, list_id = 1567015, date_from = '2021-01-01', date_to='2021-06-30'):
    results = []
    TOKEN = os.getenv('D8RKFE1p1AQEu91he5OXI5WeAKbLQcic4TmUNFda')
    for query in tqdm(queries):
        params = dict(
            token = TOKEN,
            startDate = '2021-01-01',
            endDate = '2021-06-30',
            count = 100,
            listIds = 1567015,
            searchTerm = query
        )
        res = requests.get("https://api.crowdtangle.com/posts", params = params).json()
        results += res["result"]["posts"]
        time.sleep(11)
    return pd.DataFrame(results)