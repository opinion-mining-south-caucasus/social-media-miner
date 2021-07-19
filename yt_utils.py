import os
import pandas as pd
import requests

def get_query_results_yt(queries, startdate, enddate):
    if os.environ['VERBOSE'] == 'VERBOSE':
        print('get_query_results_tw', queries, startdate, enddate)

    TOKEN = os.getenv('YOUTUBE_TOKEN')

    results = []
    for query in queries:
        first_loop = True
        while True:
            params = dict(
                part = 'snippet',
                q = query,
                maxResults = 50,
                publishedAfter = f'{startdate.isoformat()[:10]}T00:00:00Z',
                publishedBefore = f'{enddate.isoformat()[:10]}T00:00:00Z',
                key = TOKEN,
                order = 'viewCount',
            )
            if not first_loop:
                try:
                    params["pageToken"] = res_dict["nextPageToken"]
                except:
                    break
            
            first_loop = False

            res = requests.get("https://youtube.googleapis.com/youtube/v3/search", params = params)

            res_dict = res.json()
            results += [i["snippet"] for i in res_dict["items"]]
    
    df = pd.DataFrame(results)

    return df
