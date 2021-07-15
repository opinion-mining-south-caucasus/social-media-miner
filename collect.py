# !pip install langid > /dev/null 2>&1
# !git clone https://github.com/opinion-mining-for-peace/declensions.git > /dev/null 2>&1

from glob import glob
from tqdm.notebook import tqdm
import datetime
import json
from os import system
import argparse
import subprocess
import pandas as pd
import langid
from declensions.declensions import get_declensions
from transliterations.transliterate import get_transliteration

from social_media_minner.tweet_utils import *
from social_media_minner.crowdtangle_utils import *
import itertools

def get_data_tw(keywords, startdate, enddate):
    search_args = load_credentials(filename='/content/tw_keys.yaml', yaml_key="search_tweets_v2")
    queries = splitQueriesSimple(keywords)
    dfs = executeQueries(queries,'mm', startdate, search_args, period="5 days")
    return dfs

def get_data_fb(keywords, startdate, enddate):
    results = []
    queries = split_to_queries(keywords)
    df = get_query_results(queries, startdate, enddate)
    return df

def get_data_yt(keywords, startdate, enddate):
    results = []
    print(keywords, startdate, enddate)
    return results

def get_data_tl(keywords, startdate, enddate):
    results = []
    print(keywords, startdate, enddate)
    return results

def get_data_vk(keywords, startdate, enddate):
    results = []
    print(keywords, startdate, enddate)
    return results

platform_functions = {
    'tw': get_data_tw,
    'fb': get_data_fb,
    'yt': get_data_yt,
    'tl': get_data_tl,
    'vk': get_data_vk
}

def validate_keyword(keyword, platform, min_posts, max_posts):
    """
    validate_keyword check_keyword_frequency

    :param str keyword: The person sending the message
    :param int min_posts, : The recipient of the message
    :param int max_posts: The body of the message
    :return: does the keyword fit?

    The return type is `bool`.
    """

    is_keyword_valid = False
    print() 
    return is_keyword_valid


def collect(**kwargs):
    keywords, platforms, startdate, enddate, output_dir, min_posts, max_posts, use_declencions, transliterations_in = kwargs["keywords"], kwargs["platforms"], kwargs["startdate"], kwargs["enddate"], kwargs["output_dir"], kwargs["min_posts"], kwargs["max_posts"], kwargs["use_declencions"], kwargs["transliterations_in"]
    
    #detect keywod langs
    keyword_dicts = [{'keyword': keyword, "lang": langid.classify(keyword)[0]} for keyword in keywords]
    
    #Generate declencions
    if use_declencions:
        keywords_with_declencions = []
        for keyword_dict in keyword_dicts:
            declencions_for_keyword = get_declensions([keyword_dict["keyword"]], keyword_dict["lang"])
            declencions_for_keyword = [{"keyword": declencion_for_keyword, "lang": keyword_dict["lang"] } for declencion_for_keyword in declencions_for_keyword ]
            keywords_with_declencions.append(declencions_for_keyword)

        keyword_dicts = list(itertools.chain.from_iterable(keywords_with_declencions))
    
    #Generate transliterations
    if len(transliterations_in) > 0:
        transliterated_keywords = []
        for transliteration_alphabet in transliterations_in:
            for keyword_dict in keyword_dicts:
                transliterated_keywords.append({
                    "lang": keyword_dict["lang"],
                    "keyword": get_transliteration(keyword_dict["lang"], transliteration_alphabet, keyword_dict["keyword"])
                })

        keyword_dicts += transliterated_keywords
    

    keywords = [keyword_dict["keyword"] for keyword_dict in keyword_dicts]
    
    #Collect the data transliterations 
    dfs = []
    for platform in platforms:
        print(f'collecting data from - {platform}...')
        keywords_ = [i for i in keywords if validate_keyword(i, platform, min_posts, max_posts)]
        dfs += platform_functions[platform](keywords_, startdate, enddate)
    
    for result in results:
        detect_language(result)

    pd.DataFrame(results, columns=["a", "b", "c"])

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Social Media Miner')
    parser.add_argument('-k', '--keywords',   type=str, help='Comma separated list of keywords. eg: keyword1,keyword2,keyword3', required=True )
    parser.add_argument('-p', '--platforms',  type=str, help='Comma separated list of target platforms for data collection. Avaliable platforms tw,yt,fb,tl,vk', required=True)
    parser.add_argument('-o', '--output_dir', type=str, help='Directory for collected data to be stored. default=\'./\'', default='./')
    parser.add_argument('-s', '--startdate',  type=datetime.date.fromisoformat, help='Please provide the date in ISO fromat YYYY-MM-DD', required=True)
    parser.add_argument('-e', '--enddate',    type=datetime.date.fromisoformat, help='Please provide the date in ISO fromat YYYY-MM-DD', required=True)
    parser.add_argument(      '--min_posts',  type=int, help='Minimum number of posts per month per keword. default=5', default=5)
    parser.add_argument(      '--max_posts',  type=int, help='Maximum number of posts per month per keword, default=3000', default=3000)

    parser.add_argument(      '--use_declencions',  type=bool, help='Avaliable for ar,az,ka, default=False', default=False)
    parser.add_argument(      '--transliterations_in',  type=str, help='Comma separated list transliterations alphabets. Avaliable transliterations LAT,CYR, default=[]', default='')
    

    args = parser.parse_args()
    kwargs = {
        "keywords": [i.strip() for i in args.keywords.split(',') if i.strip() != ''],
        "platforms": [i.strip() for i in args.platforms.split(',') if i.strip() != ''],
        "startdate": args.startdate,
        "enddate": args.enddate,
        "output_dir": args.output_dir,
        "min_posts": args.min_posts,
        "max_posts": args.max_posts,
        "use_declencions": args.use_declencions,
        "transliterations_in": [i.strip() for i in args.transliterations_in.split(',') if i.strip() != '']
    }
    print(kwargs)
    for platform in kwargs["platforms"]:
        assert platform in ["tw","yt","fb","tl","vk"]

    for transliteration_in in kwargs["transliterations_in"]:
        assert transliteration_in in ["LAT", "CYR"]

    collect(**kwargs)
