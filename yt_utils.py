import os
import pandas as pd
import requests

def get_query_results_yt(queries, startdate, enddate):
    if os.environ['VERBOSE'] == 'VERBOSE':
        print('get_query_results_tw', queries, startdate, enddate)

    TOKEN = os.getenv('YOUTUBE_TOKEN')

    ids = []
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
            ids += [i["id"]["videoId"] for i in res_dict["items"]]
    
    df = get_video_details(ids)

    return df


def get_video_details(ids):
    if os.environ['VERBOSE'] == 'VERBOSE':
        print('get_video_details', ids)
    results = []
    
    TOKEN = os.getenv('YOUTUBE_TOKEN')
    ids_chuncks = [ids[i:i + 49] for i in range(0, len(ids), 49)]
    
    for ids_chunck in ids_chuncks:
        if os.environ['VERBOSE'] == 'VERBOSE':
            print('ids_chunck', ids_chunck)

        params = dict(
            part = 'contentDetails,id,liveStreamingDetails,localizations,recordingDetails,snippet,statistics,status,topicDetails',
            id = ','.join(ids_chunck),
            key = TOKEN,
        )

        res = requests.get("https://www.googleapis.com/youtube/v3/videos", params = params)
        res_dict = res.json()
        results += res.json()["items"]
    
    df = pd.DataFrame(results)

    for i, row in df.iterrows():
        for col in ['snippet', 'contentDetails', 'status', 'statistics', 'topicDetails']:
            if col not in row: continue
            if type(row[col]) != dict: continue
            for key in row[col]:
                if type(row[col][key]) == dict:
                    continue
                if type(row[col][key]) == list:
                    df.at[i, f'{col}_{key}'] = ','.join(row[col][key])
                    continue
                try:
                    df.at[i, f'{col}_{key}'] = row[col][key]
                except:
                    if os.environ['VERBOSE'] == 'VERBOSE':
                        print(f'cant set {col}_{key}', type(row[col][key]))
    
    return df
            