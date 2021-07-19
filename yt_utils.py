import os
import pandas as pd
import requests

def get_query_results_yt(queries):
    
    TOKEN = os.getenv('YOUTUBE_TOKEN')

    results = []
    for query in queries:
        first_loop = True
        while True:
            params = dict(
                part = 'snippet',
                q = query,
                maxResults = 50,
                publishedAfter = f'{startdate}T00:00:00Z',
                publishedBefore = f'{enddate}T00:00:00Z',
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
