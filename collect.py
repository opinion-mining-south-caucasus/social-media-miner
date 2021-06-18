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


def get_data_tw("keywords", startdate, enddate):
    results = []
    print(keywords, startdate, enddate)
    return results

def get_data_fb(keywords, startdate, enddate):
    results = []
    print(keywords, startdate, enddate)
    return results

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
    keywords, platforms, startdate, enddate, output_dir, min_posts, max_posts = kwargs["keywords"], kwargs["platforms"], kwargs["startdate"], kwargs["enddate"], kwargs["output_dir"], kwargs["min_posts"], kwargs["max_posts"]
    
    keywords = [get_translitterations(get_declencions(keyword)) for keyword in keywords]
    
    if use_declencions:
        keywords = [get_declensions([i.strip()], langid.classify(i.strip())[0]) for i in keywords]
        keywords = list(itertools.chain.from_iterable(keywords))

    for transliteration_alphabet in transliterations_in:
        # declensions_and_transliterations = [get_transliterations for ]

    results = []
    for platform in platforms:
        print(f'collecting data from - {platform}...')
        keywords_ = [i for i in keywords if validate_keyword(i, platform, min_posts, max_posts)]
        results += platform_functions[platform](keywords_, startdate, enddate)
    
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

    parser.add_argument(      '--use_declencions',  type=bool, help='Avaliable for ar,az,ka, default=False', default=false)
    parser.add_argument(      '--transliterations_in',  type=str, help='Comma separated list transliterations alphabets. Avaliable transliterations ru,en, default=[]', default=[])
    

    args = parser.parse_args()
    kwargs = {
        "keywords": [i.strip() for i in args.keywords.split(',') if i.strip() != ''],
        "platforms": [i.strip() for i in args.platforms.split(',') if i.strip() != ''],
        "startdate": args.startdate,
        "enddate": args.enddate,
        "output_dir": args.output_dir,
        "min_posts": args.min_posts,
        "max_posts": args.max_posts,
    }
    
    for platform in kwargs.platforms:
        if platform not in ["tw","yt","fb","tl","vk"]:
            raise('')

    collect(**kwargs)
