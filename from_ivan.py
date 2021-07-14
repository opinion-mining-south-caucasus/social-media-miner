import psycopg2, re, os, pickle, codecs, csv, sys, time, psycopg2
from random import shuffle, randint
from gensim.utils import simple_preprocess
from datetime import datetime
from tld import get_tld
from collections import Counter
import langid, json

#increas max int size to be able to read large csvs
maxInt = sys.maxsize
while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)


#Note: any method that loops over self.data instead of getting it from the database
#should be amended to get it from the database instead


class SocialMedia:
    group_terms = ['Erməni','հայեր','Հայկական','Ադրբեջանակա','Армянская','Армянские','азербайджанцев','Азербайджанские','ադրբեջանցիներին'] 
    vk={}
    tw={}
    curyt=None
    tot=5832.499
    yt={}
    fb={}
    vklanmap={}
    fblanmap={}
    rndlanmap={}
    authscores = {}
    important_issue=[]
    important_issue_indices=[]
    hatespeech=[]
    all_themes={}
    outpath = "output"#where it will output analysis files
    mindate = None
    categorisations = {}
    word2theme = {}
    word2lan = {}
    word2important = {}
    data = []#this was intended to hold all the posts, now deprecated in favor of database because of memory issues
    test = False
    conn2=None
    conn=None
    cur2=None
    cur=None
    hashtagregex=re.compile(r'#(\w+)', re.IGNORECASE)
    extratwittermetrics={}

    
    def __init__(self, test=False, mindate=datetime(2020,6,1).date()):
        self.categ_dict()
        self.readWordList()
        self.test = test
        self.mindate = mindate
        self.extratwittermetrics=pickle.load(open('../pres_25_5/data/pm_results.p','rb'))

    def connect(self):
        if not self.cur:
            self.conn=psycopg2.connect(database="nagorno_combined", user="postgres", password="root",port=5432)
            self.conn.set_client_encoding('UTF8')
            self.cur = self.conn.cursor()
    
    def connect2(self):
        #2nd connection for when we want to write to the database in a read loop
        if not self.cur2:
            self.conn2=psycopg2.connect(database="nagorno_combined", user="postgres", password="root",port=5432)
            self.conn2.set_client_encoding('UTF8')
            self.cur2 = self.conn2.cursor()

    def fixUrl(self, s):
        s=s.replace('\u200b', ' ').strip()
        for z in [',','.',')','\u200b']:
            if s.endswith(z):
                s=s[:-1]
        s=s.replace('\u200b', ' ').strip()
        if s.startswith('https://https://'):
            s=s.replace('https://https://', 'https://')
        if s.startswith('https://www.instagram'):
            s='https://www.instagram.com'
        if s.startswith('http://www.atvm.tv'):
            s='https://atvm.tv'
        if s == 'https://voiceofkarabakh.co]':
            s='https://voiceofkarabakh.com'
        if s.startswith('https://www.antenn.az'):
            s='https://antenn.az'
        if s.startswith('https://t.co…'):
            s='https://t.co'
        if s.startswith('https://ria.ru'):
            s='https://ria.ru'
        return s
    
    def insertData(self):
        #Inserts data into the database
        self.connect()
        self.connect2()
        ct=0
        tot=len(self.data)
        shuffle(self.data)
        for itm in self.data:
            ct+=1
            if ct % 1000 == True:
                print(ct, tot, itm['platform'])
            itm['message_id'] = itm['id']
            
            self.cur2.execute("select message_id from posts where platform = '%s' and message_id = '%s'" % (itm['platform'], itm['message_id']))
            row2 = self.cur2.fetchone()
            if not row2:
                try:
                    self.cur.execute("""insert into posts (
                        message_id, platform, date, week, month, score, text, language, usr, category, important_issue, tags, url)
                        values
                        (%(message_id)s, %(platform)s, %(date)s, %(week)s, %(month)s, %(score)s, %(text)s, %(language)s, %(user)s, %(category)s, %(important_issue)s, %(tags)s, %(url)s)""",
                        itm)
                except Exception as err:
                    print(itm)
                    print(err)
                    sys.exit()
            if ct % 1000 == True:
                self.conn.commit()
        self.conn.commit()
    
    def alignData(self, platforms=['vk','tw','yt','fb','tl']):
        #Aligns all the data from the different platforms so that it has the same format
        print("Start align")
        qs=[]   
        ct = 0
        self.connect()
        
        #Cache the language data, as realtime detection is slow
        langdata = pickle.load(open('langdata.p','rb'))
        facebook_emotes={}
        langdatachanged=False
        for platform in platforms:
            dic = getattr(self, platform)
            tot = len(dic)
            keys = list(dic.keys())
            for message_id in keys:
                v = dic[message_id]
                ct+=1
                if ct % 1000 == True:
                    if tot > 1000000:
                        print(platform, "%s/%s"% (ct/1000000,tot/1000000))
                    else:
                        print(platform, "%s/%s"% (ct/1000,tot/1000))
                dt = self.getDate(v)
                if not dt:#undated stuff is no use
                    continue
                if len(v) < 4:#too much missing data
                    continue
                text = self.getText(v)
                v['id'] = message_id
                usr = self.getUser(v)
                lankey= "%s_%s"% (platform, message_id)
                try:
                    lan=langdata[lankey]
                except KeyError:
                    lan = self.detect_lang(text)
                    langdata[lankey]=lan
                    langdatachanged=True
                if lan == 'hy':
                    lan = 'arm'
                if lan == 'tr':
                    lan = 'az'
                    
                ddic = {
                    'id': message_id,
                    'date': dt,
                    'week': "%s-%s"% (dt.year, str(dt.isocalendar()[1]).zfill(2)),
                    'month': "%s-%s"% (dt.year, str(dt.month).zfill(2)),
                    'score': self.getScore(v, platform),
                    'text': text,
                    'language': lan,
                    'user': usr,
                    'category': self.getCat(v),
                    'important_issue': self.getImportantIssue(v, text),
                    'platform': platform,
                    'tags': self.getTags(v),
                    'url': self.getUrl(message_id, platform, v)
                }
                
                #hack to include FB emotion data, TODO: integrate into the regular menu
                ooh={}
                if platform == 'fb':
                    for xy in ['Love','Wow','Haha','Sad','Angry','Care']:
                        ddic[xy] = int(v[xy])
                        ooh[xy] = int(v[xy])
                facebook_emotes[message_id] = ooh
                    

                if ct % 1000 == True:
                    self.conn.commit()
                ddic['message_id'] = ddic['id']

                del dic[message_id]
        self.conn.commit()
        if langdatachanged:
            pickle.dump(langdata, open('langdata.p','wb'))

    def findPhrases(self):
        #Trains phrase recognizer on the data
        from gensim.models import Phrases

        self.tokenize()
        bigram = Phrases(self.tokenized, min_count=10, threshold=2)
        bigram.save('d:/phrases.bin')

    def makew2v(self):
        #Creates Word2Vec from the data
        #This did not work very well because the dataset is too small really
        from gensim.models import Word2Vec
        import logging
        self.tokenize()
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

        model = Word2Vec(self.tokenized, min_count = 3,  iter=5, workers=4, size = 150, window = 10) 
        model.wv.save_word2vec_format('d:/nagornow2v.bin', binary=True)

    def parsew2v(self):
        #Uses trained word2vec to create a csv of 'most similar' words, those are still hardcoded
        from gensim.models import KeyedVectors
        model = KeyedVectors.load_word2vec_format("d:/nagornow2v.bin", binary=True)
        wantwords=['Ադրբեջան', 'Ադրբեջանցի', 'Qarabağ', 'Erməni', 'Ermənistan']
        out=[]
        for x in wantwords:
            print(x)
            try:
                for q in model.most_similar(x.lower(), topn=20):
                    out.append([x] + list(q))
                    print([x] + list(q))
            except KeyError:
                print("NOT IN")
        with codecs.open('most_similar_w2v.csv','w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out)

    def process_phrases(self):
        #This tries to recognize common phrases in the corpus and outputs a csv with them
        from gensim.models import Phrases
        bigram = Phrases.load('d:/phrases.bin')
        self.group_terms = [g.lower().strip() for g in self.group_terms]
        out=[]
        tot=len(self.data)
        for i, sent in enumerate(self.data):
            if i % 10000 == True:
                print("%s/%s"% (i/1000,tot/1000))
            q = [a for a in bigram[simple_preprocess(sent['text'])] if '_' in a]
            if not q == []:
                for qq in q:
                    for z in self.group_terms:
                        if z in qq:
                            out.append(qq)
        from collections import Counter
        c=Counter(out)
        out2=[]
        for k,v in c.items():
            if v > 10:
                out2.append(k.split('_')+ [ v])
        with codecs.open('possible_hatespeech.csv','w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(out2)

    def tokenize(self):
        #Tokenizes the data (for word2vec etc)
        self.tokenized=[]
        for d in self.data:
            self.tokenized.append(simple_preprocess(d['text']))

    def grabAuthors(self):
        #Gets all the authors from the data, only used this to distill unique twitter users for bot detection
        auts=[]
        for d in self.data:
            auts.append(d['user'])
        auts= set(auts)
        print(len(auts), 'total authors')
        pickle.dump(auts, open('authors.p','wb'))

    def getCounts(self):
        #Counts number of posts per platform
        dic = {
            'yt':0,
            'fb':0,
            'tw':0,
            'vk':0,
        }
        for itm in self.data:
            dic[itm['platform']]+=1
        print(dic)

    def getCountsPerLanguage(self):
        #Counts numbers of posts per platform per language
        dic = {
            'yt':{},
            'fb':{},
            'tw':{},
            'vk':{},
        }
        for itm in self.data:
            try:
                dic[itm['platform']][itm['language']]+=1
            except KeyError:
                dic[itm['platform']][itm['language']]=1
        for k,v in dic.items():
            print(k)
            for kk, vv in v.items():
                print([k,kk,vv])

    def getUrl(self, message_id, platform, v):
        #Creates a link to the post
        if platform == 'fb':
            return v['URL']
        elif platform == 'yt':
            return 'https://www.youtube.com/watch?v=%s' % message_id
        elif platform == 'tw':
            return 'https://twitter.com/rassam45/status/%s'% message_id
        else:
            return '?'

    def countImportantIssuePerLanguage(self):
        #not finished, so probably not working
        print('countImportantIssuePerLanguage')
        out=[['platform', 'month', 'term', 'no_items', 'no_unique_items', 'engagement','Theme', 'Partisanship','Language','Date']]
        unique_woink={}
        woink={}
        wm={}
        engmap={}
        wct=0
        tot=len(self.data)
        for i, itm in enumerate(self.data):
            scored=False
            wct+=1
            if wct % 1000 == True:
                print(wct, tot)
            for theme, termlist in self.all_themes.items():
                for wordding in termlist:
                    words = wordding[0]
                    themeee = wordding[1]
                    partizanship = wordding[2]
                    language = wordding[3]
                    Date = wordding[4]
                    need = wordding[5]
                    if not need or need in itm['text'].lower():
                        for word in words:
                            if not word.strip():#just in case
                                continue
                            if word.lower().strip() in itm['text'].lower():
                                if re.search(r'\b%s\b' % word.lower().strip(), itm['text'].lower()):
                                    key = (itm['platform'], itm['month'], word)
                                    wm[word] = [theme]+termlist[1:]
                                    wm[word] = [themeee, partizanship, language, Date]
                                    try:
                                        woink[key] += 1
                                    except KeyError:
                                        woink[key] = 1
                                    
                                    if not scored:
                                        score = itm['score']
                                        if score < 0:
                                            score=0
                                        try:
                                            engmap[key] += score
                                        except KeyError:
                                            engmap[key] = score
                                        try:
                                            unique_woink[key] += 1
                                        except KeyError:
                                            unique_woink[key] = 1
                                        scored=True
        for k,v in woink.items():
            out.append(list(k) + [v, unique_woink[k], engmap[k]] +  wm[list(k)[-1]])
        self.writecsv("%s/allThemes.csv"%self.outpath, out)

    def readWordList(self):
        #Reads the relevant thematic etc lists into usable variables
        with codecs.open('../pres_23_4_2021/twitter/newcollection/questionlangs-edited.csv', 'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=',')
            for row in r:
                kk = "%s_%s_%s" % (row['platform'], row['user'], row['id'])
                self.rndlanmap[kk] = row['language']

        #This will crash, because the file format changed. Important issue currently
        #missing from the document - or is it 'partisanship'?
        with codecs.open('nk_keywords_final.csv', 'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                need=False
                for x in ['Word, hashtag or issue', 'Theme', 'Partisanship','Language']:
                    row[x] = row[x].lower().strip()
                if row['Theme']  in ['confrontational', 'hate speech']:
                    self.hatespeech.append(row)
                elif row['Theme']  in ['important issue']:
                    self.important_issue.append(ddd)
                try:
                    self.all_themes[row['Theme']].append(row)
                except KeyError:
                    self.all_themes[row['Theme']] = [row]


        with codecs.open('../pres_23_4_2021/twitter/newcollection/get_vk.csv', 'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=',')
            for row in r:
                self.word2theme[row['vk_query'].strip().lower()] = row['Theme (likely ambiguous)']
                self.word2lan[row['vk_query'].strip().lower()] = row['Language']
                self.word2important[row['vk_query'].strip().lower()] = row['important_to']

    def readVk(self):
        #Loads the VK data from the database and aligns it
        vk_dictionary={}
        conn=psycopg2.connect(database="nagorno2", user="postgres", password="root",port=5432)
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        if self.test:
            cur.execute("select * from results limit 200")
        else:
            cur.execute("select * from results")
        row=cur.fetchone()
        flds = [a[0] for a in cur.description]
        ct=0
        while row:
            row = dict(zip(flds, row))
            ct+=1
            if ct % 10000 == True:
                print('Read vk', ct)
            vk_dictionary[row['id']] = row
            row=cur.fetchone()
        conn.close()
        conn=psycopg2.connect(database="nagorno_walls", user="postgres", password="root",port=5432)
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        if self.test:
            cur.execute("select * from results limit 200")
        else:
            cur.execute("select * from results")
        row=cur.fetchone()
        flds = [a[0] for a in cur.description]
        ct=0
        while row:
            row = dict(zip(flds, row))
            ct+=1
            if ct % 10000 == True:
                print('Read vk', ct)
            vk_dictionary[row['id']] = row
            row=cur.fetchone()
        
        self.vk = vk_dictionary
        self.alignData(['vk'])
        self.vk = {}

    def findDelimiter(self, directory, x):
        #Hacky attempt to determine delimiter of csv file as different files use different ones.
        #TODO: replace by a better tested function (pandas?)
        delim = '\t'
        with codecs.open('%s/%s' % (directory, x), 'r', encoding='utf-8') as f:
            for l in f:
                tabct = l.count('\t')
                semict = l.count(';')
                comcount = l.count(',')
                maxi = max([tabct, semict, comcount])
                if maxi == semict:
                    delim=';'
                elif maxi == comcount:
                    delim=','
                break
        return delim

    def readTwitter(self, directory, delete=True, align=True, skipFields=True):
        #Reads Twitter data from json and csv files.
        #TODO: integrate the duplicate loop so json and csv are parsed by the same code
    
        #NOTE: Replace character that shows up in Notepad++ as LS with space in the files
        #or reading will error out, removed those manually for now. TODO: automate
        
        #drop irrelevant fields to save memory 
        needed_fields = ('id','id_str','body','text','created_at','author_id','timestamp', 'source', 'retweet_count','reply_count','like_count','quote_count','public_metrics')

        tw_dictionary={}
        ct=0
        aa = os.listdir(directory)
        for x in aa:
            #these have bad tweet ids (mangled by Excel or something), so skipping
            if 'az_full_denoised_with_langs.csv' in x or 'data_az_hy_cleaned_kws_jan_feb_mar0_lang_filtered' in x:
                continue
            if not x.endswith('.csv'):
                if x.endswith('.ndjson'):
                    rct=0
                    with codecs.open('%s/%s' % (directory, x), 'r', encoding='utf-8') as f:
                        for l in f:
                            if not l.strip():
                                continue
                            rrow = json.loads(l)
                            row={}
                            if skipFields:
                                for ff in needed_fields:
                                    try:
                                        row[ff] = rrow[ff]
                                    except KeyError:
                                        pass
                            else:
                                row = rrow
                            rct+=1
                            if rct % 10000 == True:
                                print(rct, x)
                            le_id=None
                            try:
                                le_id = row['id_str']
                            except KeyError:
                                pass
                            if not le_id:
                                try:
                                    le_id = row['id']
                                except KeyError:
                                    print('row:', [row])
                                    sys.exit('!!!!!!!!')
                            try:
                                row['id'] = le_id
                                int(float(le_id))
                            except Exception as err:
                                errct+=1
                                #these are tail ends of previous lines, fixed some manually but not all
                                continue
                            tw_dictionary[row['id']] = row
                    continue
                else:
                    continue
            
            if self.test and ct > 200:
                break
            print(x, len(tw_dictionary))
            
            delim = self.findDelimiter(directory, x)
            rct=0
            errct=0
            err_linenos=[]
            prevrow=None
            with codecs.open('%s/%s' % (directory, x), 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=delim)
                for rrow in r:
                    row={}
                    if skipFields:
                        for ff in needed_fields:
                            try:
                                row[ff] = rrow[ff]
                            except KeyError:
                                pass
                    else:
                        row = rrow

                    rct+=1
                    try:
                        le_id = row['id_str']
                    except KeyError:
                        le_id = row['id']
                    row['id'] = le_id
                    try:
                        int(float(le_id))
                    except Exception as err:
                        errct+=1
                        continue
                    try:
                        tw_dictionary[le_id] = row
                    except KeyError:
                        print("Bad twitter file:", x)
                        sys.exit()
                    if self.test and ct > 200:
                        break
                    ct+=1
                    prevrow= row
            
            print(errct, 'errors!')
            if errct > 0:
                
                print('ERRS FOUND')
        self.tw = tw_dictionary
        if align:
            self.alignData(['tw'])
        if delete:
            self.tw = {}

    def addTwitterMetrics(self, directory):
        #Adds Twitter metrics to posts that don't have them.
        #Was needed because some data was downloaded with, other without metrics.
        #As we have duplicate posts in there, we can update the ones that don't have metrics with the data that has in some cases
        #Newscore is the column that gets the updated scores (just to be safe, didn't want to overwrite the scores column).
        #Would not fix the double loop: just only download data WITH metrics next time so we can deprecate this method
        self.connect()
        wants={}
        self.cur.execute("select message_id from posts where platform = 'tw' and score < 0 and newscore is null")
        row=self.cur.fetchone()
        while row:
            wants[row[0]] = None
            row=self.cur.fetchone()
        print(len(wants), 'to fix')
        ct=0
        if True:
            tw_dictionary={}
            ct=0
            added=0
            aa = os.listdir(directory)
            for x in aa:
                
                if not x.endswith('.csv'):
                    if x.endswith('.ndjson'):
                        rct=0
                        with codecs.open('%s/%s' % (directory, x), 'r', encoding='utf-8') as f:
                            for l in f:
                                if not l.strip():
                                    continue
                                row = json.loads(l)
                               
                                rct+=1
                                if rct % 10000 == True:
                                    print(rct, x)
                                le_id=None
                                try:
                                    le_id = row['id_str']
                                except KeyError:
                                    pass
                                if not le_id:
                                    try:
                                        le_id = row['id']
                                    except KeyError:
                                        print('row:', [row])
                                        sys.exit('!!!!!!!!')
                                try:
                                    row['id'] = le_id
                                    int(float(le_id))
                                except Exception as err:
                                    errct+=1
                                    continue
                                try:
                                    if not wants[row['id']]:
                                        score = self.getScore(row, 'tw')
                                        if score > 0:
                                            self.cur.execute("update posts set newscore = %s where message_id = '%s' and platform = 'tw'" % (score, row['id']))
                                            if ct % 1000 == True:
                                                self.conn.commit()
                                            added+=1
                                except KeyError:
                                    pass
                        continue
                    else:
                        continue
                
                if self.test and ct > 200:
                    break
                print(x, len(tw_dictionary))
                delim = self.findDelimiter(directory, x)
                rct=0
                errct=0
                err_linenos=[]
                prevrow=None
                with codecs.open('%s/%s' % (directory, x), 'r', encoding='utf-8') as f:
                    r=csv.DictReader(f, delimiter=delim)
                    for row in r:

                        rct+=1
                        le_id=None
                        try:
                            le_id = row['id_str']
                        except KeyError:
                            pass
                        if not le_id:
                            try:
                                le_id = row['id']
                            except KeyError:
                                print('row:', [row])
                                sys.exit('!!!!!!!!')
                        try:
                            row['id'] = le_id
                            int(float(le_id))
                        except Exception as err:
                            errct+=1
                            continue
                        try:
                            if not wants[row['id']]:
                                score = self.getScore(row, 'tw')
                                if score > 0:
                                    
                                    self.cur.execute("update posts set newscore = %s where message_id = '%s' and platform = 'tw'" % (score, row['id']))
                                    if ct % 1000 == True:
                                        self.conn.commit()

                                    added+=1
                        except KeyError:
                            pass
                        ct+=1
                        prevrow= row
                self.conn.commit()
                print(errct, 'errors!', 'added:', added)
                if errct > 0:
                    
                    print('ERRS FOUND')
            self.conn.commit()
            print(added, 'total added done')
            sys.exit()
            
    def readYouTube(self, directory):
        #Reads the YouTube csvs and aligns them
        yt_dictionary={}
        ct=0
        for x in os.listdir(directory):
            if not x.endswith('.csv'):
                continue
            print(x)
            self.curyt=x
            delim = self.findDelimiter(directory, x)
            with codecs.open('%s/%s'% (directory, x), 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=delim)
                for row in r:
                    row['fn'] = x
                    try:
                        yt_dictionary[row['videoId']]
                    except KeyError:
                        yt_dictionary[row['videoId']] = row
                    if self.test and ct > 200:
                        break
                    ct+=1
        self.yt = yt_dictionary
        self.alignData(['yt'])
        self.yt = {}

    def readTelegram(self, directory):
        #Reads the Telegram csvs and aligns them
        tl_dictionary={}
        ct=0
        for x in os.listdir(directory):
            if not x.endswith('.csv'):
                continue
            print(x)
            with codecs.open('%s/%s'% (directory, x), 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=',')
                for row in r:
                    row['fn'] = x
                    try:
                        tl_dictionary[row['id']]
                    except KeyError:
                        tl_dictionary[row['id']] = row
                    if self.test and ct > 200:
                        break
                    ct+=1
        self.tl = tl_dictionary
        self.alignData(['tl'])
        self.fb = {}

    def readFacebook(self, directory):
        #Reads Facebook csvs and aligns them
        fb_dictionary={}
        ct=0
        for x in os.listdir(directory):
            if not x.endswith('.csv'):
                continue
            print(x)
            with codecs.open('%s/%s'% (directory, x), 'r', encoding='utf-8') as f:
                r=csv.DictReader(f, delimiter=',')
                for row in r:
                    row['fn'] = x
                    try:
                        fb_dictionary[row['Facebook Id']]
                    except KeyError:
                        fb_dictionary[row['Facebook Id']] = row
                    if self.test and ct > 200:
                        break
                    ct+=1
        self.fb = fb_dictionary
        self.alignData(['fb'])
        self.fb = {}

    def detect_lang(self, text):
        #Detects the language of the post
        lan = None
        try:
            lan = langid.classify(text)[0]
        except Exception as err:
            print(err)
        return lan

    def getDate(self, dic):
        #Tries to figure out the appropriate date for the post according to platform and field name
        dt = None
        datefound = False
        for fld in ['date', 'created_at', 'Page Created', 'publishedAt', 'timestamp']:
            try:
                if dic[fld] is None:
                    return None
                try:
                    dt = datetime.utcfromtimestamp(int(dic[fld])).strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        dt = datetime.strptime(dic[fld], '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # or 2020-12-30T22:21:45.000Z
                        try:
                            if dic[fld] in ['', None]:
                                return None
                            dt = datetime.strptime(dic[fld][:19].replace(' ','T'), '%Y-%m-%dT%H:%M:%S')
                        except ValueError as err:
                            print(err, dic)
                            print('Badd date: "%s"' % dic[fld])
                            # sys.exit("Date error")
                            return None

                datefound = True
                break
            except KeyError:
                pass
        if not datefound:
            print('DATE ERR', dic)
            sys.exit('!!!')
        dt = str(dt)
        if dt:
            try:
                dt = datetime.strptime(dt[:10], "%Y-%m-%d").date()
            except Exception as err:
                print(err)
                dt=None
        try:
            if dt is not None and dt < self.mindate:
                dt=None
        except Exception as err:
            dt=None
            print("Date problem: '%s'" % dt, err)
        return dt

    def getTags(self, dic):
        #this seems pretty useless
        try:
            return dic['tags'].split(',')
        except KeyError:
            return []

    def no_posts_per_partisanship(self):
        #Outputs a file counting posts per partisanship
        print('no_posts_per_partisanship')
        out=[['partisanship','no_posts']]
        woink={}
        self.connect()
        mapper = self.readMapper()
        self.cur.execute("select id from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        i=0
        
        while itmr:
            i+=1
            if i % 100000 == True:
                print(i,'nppp!')
            itm = dict(zip(desc, itmr))
            pship=[]
            try:
                mapper[itm['id']]
                for theme, rows in self.all_themes.items():
                    for row in rows:
                        term = self.makeWordOrIssue(row)
                        if term in mapper[itm['id']]:
                            pship.append(row['Partisanship'])
            except KeyError:
                pass
            for x in set(pship):
                try:
                    woink[x]+=1
                except KeyError:
                    woink[x]=1
            itmr=self.cur.fetchone()

        for k,v in woink.items():
            out.append( [k, v])
        self.writecsv("%s/no_posts_per_partisanship.csv"%self.outpath, out)
        print(i)
    
    def Cross_platform_prominence_of_posts(self):
        #Outputs a file with engagement metrics per platform per language over time
        print('Cross_platform_prominence_of_posts')
        out=[['date','platform','language','partisanship','engagement_score','no_posts']]
        woink={}
        self.connect()
        mapper = self.readMapper()
        self.cur.execute("select id, date, platform, score, newscore, language from posts where score > 0 or newscore > 0")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        i=0
        gabbake={}
        
        while itmr:
            i+=1
            addered=False
            if i % 100000 == True:
                print(i,'CPP!')
            itm = dict(zip(desc, itmr))
            itm['score'] = itm['score']
            if not itm['language'] in ['az','arm','en','ru']:
                itm['language'] = 'other'
            if itm['newscore']:
                itm['score'] = itm['newscore']
            if itm['score'] >= 0:
                pship=[]
                try:
                    mapper[itm['id']]
                    for theme, rows in self.all_themes.items():
                        for row in rows:
                            term = self.makeWordOrIssue(row)
                            if term in mapper[itm['id']]:
                                pship.append(row['Partisanship'])
                except KeyError:
                    pass

                        
                try:
                    key = (itm['date'], itm['platform'], itm['language'], ",".join(list(set(pship))))
                    if not addered:
                        addered=True
                        try:
                            gabbake[key]+=1
                        except KeyError:
                            gabbake[key] = 1
                    try:
                        woink[key] += itm['score']
                    except KeyError:
                        woink[key] = itm['score']
                except KeyError:
                    pass
            itmr = self.cur.fetchone()
        for k,v in woink.items():
            out.append(list(k) + [v, gabbake[k]])
        self.writecsv("%s/Cross_platform_prominence_of_posts.csv"%self.outpath, out)
        print(i, 'total posts used')#4021586 total posts used
    
    def Hatespeech(self):
        #TODO: deprecate into allThemes
        #we did not end up using this, was intended to output hatespeech per platform per month
        print('Hatespeech')
        out=[['platform', 'month', 'term', 'no_items','Theme', 'Partisanship','Language']]
        woink={}
        wm={}
        for i, itm in enumerate(self.data):
            for termlist in self.hatespeech:
                word = termlist[0]
                if word in itm['text'].lower():
                    if re.search(r'\b%s\b' % word, itm['text'].lower()):
                        key = (itm['platform'], itm['month'], word)
                        wm[word] = termlist[1:]
                        try:
                            woink[key] += 1
                        except KeyError:
                            woink[key] = 1
        for k,v in woink.items():
            out.append(list(k) + [v] +  wm[list(k)[-1]])
        self.writecsv("%s/Hatespeech.csv"%self.outpath, out)
    
    def Url_mediadiet(self, dryrun=False):
        #Creates a graph that links languages to the urls mentioned
        import requests
        print('Url_mediadiet')
        urlrex = re.compile(r'(https?://[^\s]+)')
        
        self.connect()
        self.cur.execute("select text from posts")
        itmr = self.cur.fetchone()
        ct=0
        
        #The below to find urls to resolve elsewhere (because multiple seemingly different urls may point to the same location)
        if False:
            urls=[]
            while itmr:
                ct+=1
                if ct % 10000 == True:
                    print(ct/1000, self.tot)
                    urls=list(set(urls))
                if ct % 100000 == True:
                    pickle.dump(urls, open('urls2check.p','wb'))
                for url in re.findall(urlrex, itmr[0]):
                    urls.append(url)
                itmr = self.cur.fetchone()
            pickle.dump(urls, open('urls2check.p','wb'))
            print(ct)
            sys.exit('done')


        tlds={}
        import networkx as nx
        G = nx.Graph()
        ld=False
        cache=pickle.load(open('url_resolve_cache_w_tld.p','rb'))
        ct=0
        tot = len(self.data)/1000
        shuffle(self.data)
        urldataChanged=False
        self.cur.execute("select text, language, platform from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        while itmr:
            itm = dict(zip(desc, itmr))

            ct+=1
            if ct % 100 == True and urldataChanged:
                #hedge for multiple processes trying to read/write the same cache file
                ld=False
                while not ld:
                    try:
                        cache2 = pickle.load(open('url_resolve_cache_w_tld.p','rb'))
                        ld=True
                        for k,v in cache2.items():
                            cache[k] = v
                    except Exception as err:
                        print(err)
                        time.sleep(1)
                pickle.dump(cache, open('url_resolve_cache_w_tld.p','wb'))
                if randint(0,10) == 5:
                    pickle.dump(cache, open('url_resolve_cache_w_tld_bu.p','wb'))
                    print('DUMPED')
                urldataChanged=False

            if (ct % 100 == True) or (itm['platform'] == 'yt' and ct % 10 ==True):
                print("%s/%s"% (ct/1000, tot), 'url', itm['platform'], self.curyt)

            if not itm['language'] in ['en','arm','az','ru']:
                itmr = self.cur.fetchone()
                continue
            
            for url in re.findall(urlrex, itm['text']):
                try:
                    url = cache[url]
                except KeyError:
                    urldataChanged=True
                    try:
                        print(url)
                        fp = requests.get(url, timeout=30)

                        realurl = fp.url
                        url=realurl
                        try:
                            vv=self.fixUrl(url)
                            res = get_tld(vv, as_object=True)
                            domain = res.domain
                            cache[orurl] = (url,domain)
                            cache[url] = (url,domain)
                        except Exception as rr:
                            print(rr)
                            cache[orurl] = (url,None)
                    except Exception:
                        cache[orurl] = (url,None)
                if not dryrun:
                    domain=url
                    try:
                        try:
                            domain = tlds[url]
                        except KeyError:
                            res = get_tld(url, as_object=True)
                            domain = res.domain
                            tlds[url] = domain
                    except Exception:
                        tlds[url] = url
                        
                    c = [itm['language'], domain]
                    if G.has_edge(c[0], c[1]):
                        G[c[0]][c[1]]['weight']+=1
                    else:
                        G.add_edge(c[1], c[0], weight=1)
                        G[c[0]][c[1]]['language'] = c[0]
            itmr = self.cur.fetchone()
        if urldataChanged:
            ld=False
            try:
                cache2 = pickle.load(open('url_resolve_cache.p','rb'))
                ld=True
                for k,v in cache2.items():
                    cache[k] = v
            except Exception:
                time.sleep(1)
        
            pickle.dump(cache, open('url_resolve_cache.p','wb'))
        if not dryrun:
            nx.write_gexf(G, '%s/Url_mediadiet.gexf' % (self.outpath,))
    
    def Url_mediadiet_partisanship(self):
        #Creates a graph linking mentioned urls to partisanship
        import requests
        print('Url_mediadietPARTISAN')
        urlrex = re.compile(r'(https?://[^\s]+)')
        tlds={}
        import networkx as nx
        G = nx.Graph()
        cache=pickle.load(open('url_resolve_cache_w_tld.p','rb'))
        ct=0        
        self.connect()
        mapper = self.readMapper()
        self.cur.execute("select text, id from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        while itmr:
            itm = dict(zip(desc, itmr))

            ct+=1
            if ct % 10000 == True:
                print(ct)
            try:
                mapper[itm['id']]
            except KeyError:
                itmr=self.cur.fetchone()
                continue
            pship=[]
            for theme, rows in self.all_themes.items():
                for row in rows:
                    term = self.makeWordOrIssue(row)

                    word = row['Word, hashtag or issue'].lower()
                    if term in mapper[itm['id']]:
                        pship.append(row['Partisanship'])
                
            for url in re.findall(urlrex, itm['text']):
                try:
                    try:
                        url,domain = cache[url]
                    except ValueError:
                        url = cache[url]
                        try:
                            res = get_tld(url, as_object=True)
                            domain = res.domain
                        except Exception:
                            domain = url
                except KeyError:
                    sys.exit('Not found!')
                    try:
                        fp = requests.get(url)
                        realurl = fp.url
                        cache[url] = realurl
                        url=realurl
                    except Exception as err:
                        print(err)
                for pp in set(pship):
                    c = [pp, domain]
                    if G.has_edge(c[0], c[1]):
                        G[c[0]][c[1]]['weight']+=1
                    else:
                        G.add_edge(c[1], c[0], weight=1)
                        G[c[0]][c[1]]['partisanship'] = c[0]
            itmr=self.cur.fetchone()
        pickle.dump(cache, open('url_resolve_cache.p','wb'))
        nx.write_gexf(G, '%s/Url_mediadiet_partisanship.gexf' % (self.outpath,))
    
    def UrlsInPosts(self, onlyImportantIssues=False):
        #Outputs a network between languages and mentioned TLDs
        #This seems to be a duplicate of Url_mediadiet, so IIRC is't deprecated
        print('UrlsInPosts')
        urlrex = re.compile(r'(https?://[^\s]+)')
        tlds={}
        import networkx as nx
        import networkx as nx
        G = nx.Graph()

        for i, itm in enumerate(self.data):
            if onlyImportantIssues and not i in self.important_issue_indices:
                continue
            for url in re.findall(urlrex, itm['text']):
                try:
                    try:
                        domain = tlds[url]
                    except KeyError:
                        res = get_tld(url, as_object=True)
                        domain = res.domain
                        tlds[url] = domain
                except Exception:
                    tlds[url] = url
            
                c = [itm['language'], domain]
                if G.has_edge(c[0], c[1]):
                    G[c[0]][c[1]]['weight']+=1
                else:
                    G.add_edge(c[1], c[0], weight=1)
                    G[c[0]][c[1]]['language'] = c[0]
        nx.write_gexf(G, '%s/networks/urls_in_text.gexf' % (self.outpath,))
    
    def ImportantIssue(self):
        #TODO: deprecate into allThemes
        #Outputs a csv with the important issues and their themes, partisanships and languages
        print('ImportantIssue')
        out=[['platform', 'month', 'term', 'no_items','Theme', 'Partisanship','Language']]
        woink={}
        wm={}
        postcounts={}
        
        for i, itm in enumerate(self.data):
            for termlist in self.important_issue:
                word = termlist[0]
                if word in itm['text'].lower():
                    if re.search(r'\b%s\b' % word, itm['text'].lower()):
                        self.important_issue_indices.append(i)
                        key = (itm['platform'], itm['month'], word)
                        wm[word] = termlist[1:]
                        try:
                            woink[key] += 1
                        except KeyError:
                            woink[key] = 1
        for k,v in woink.items():
            out.append(list(k) + [v] +  wm[list(k)[-1]])
        self.writecsv("%s/ImportantIssue.csv"%self.outpath, out)
        self.important_issue_indices=set(self.important_issue_indices)
    
    def allThemes(self):
        #Outputs a csv with an overview of the themes and other relevant variables
        print('allThemes')
        out=[['platform', 'month', 'term', 'no_items', 'no_unique_items', 'engagement','Theme', 'Partisanship','Language','Date']]
        unique_woink={}
        woink={}
        wm={}
        engmap={}
        wct=0
        tot=len(self.data)
        for i, itm in enumerate(self.data):
            scored=False
            wct+=1
            if wct % 1000 == True:
                print(wct, tot)
            for theme, termlist in self.all_themes.items():
                for wordding in termlist:

                    words = wordding[0]
                    themeee = wordding[1]
                    partizanship = wordding[2]
                    language = wordding[3]
                    Date = wordding[4]
                    need = wordding[5]
                    if not need or need in itm['text'].lower():
                        for word in words:
                            if not word.strip():#just in case
                                continue
                            if word.lower().strip() in itm['text'].lower():
                                if re.search(r'\b%s\b' % word.lower().strip(), itm['text'].lower()):
                                    key = (itm['platform'], itm['month'], word)
                                    wm[word] = [theme]+termlist[1:]
                                    wm[word] = [themeee, partizanship, language, Date]
                                    try:
                                        woink[key] += 1
                                    except KeyError:
                                        woink[key] = 1
                                    
                                    if not scored:
                                        score = itm['score']
                                        if score < 0:
                                            score=0
                                        try:
                                            engmap[key] += score
                                        except KeyError:
                                            engmap[key] = score
                                        try:
                                            unique_woink[key] += 1
                                        except KeyError:
                                            unique_woink[key] = 1
                                        scored=True
        for k,v in woink.items():
            out.append(list(k) + [v, unique_woink[k], engmap[k]] +  wm[list(k)[-1]])
        self.writecsv("%s/allThemes.csv"%self.outpath, out)

    def Most_frequent_keywords_queries_per_platform(self):
        #Outputs a csv with, you guessed it, the most frequent keywords per platform
        print('Most_frequent_keywords_queries_per_platform')
        out=[['platform', 'month', 'term', 'language','partisanship', 'no_items']]
        woink={}
        self.connect()
        mapper =self.readMapper()
        ct=0
        self.cur.execute("select platform, month, text, language, id from posts")
        desc = [a[0] for a in self.cur.description]
        i=0
        itmr = self.cur.fetchone()
        while itmr:
            itm = dict(zip(desc, itmr))
            i+=1
            if i % 1000 == True:
                print(i/1000, 'Most_frequent_keywords_queries_per_platform')
            try:
                mapper[itm['id']]
            except KeyError:
                itmr=self.cur.fetchone()
                continue
            txt=itm['text'].lower()
            donethemes = []
            for theme, rows in self.all_themes.items():
                for row in rows:
                    word = "%s %s" % (row['Word, hashtag or issue'].lower(), row['AND'].lower())
                    word = word.lower().strip()
                    
                    term = self.makeWordOrIssue(row)
                    

                    key = (itm['platform'], itm['month'], itm['language'], row['Partisanship'], word)
                    if key in donethemes:
                        continue
                    word = row['Word, hashtag or issue'].strip().lower()
                    if term in mapper[itm['id']]:
                            try:
                                woink[key] += 1
                            except KeyError:
                                woink[key] = 1
                            donethemes.append(key)
            itmr = self.cur.fetchone()
        
        for k,v in woink.items():
            out.append(list(k) + [v])
        self.writecsv("%s/Most_frequent_keywords_queries_per_platform.csv"%self.outpath, out)

    def confrontational_hatespeech_incitement(self, wanted_cats=['confrontational','hate speech', 'incitement']):
        #Outputs confrontational, hatespeech and incitement conveniently rolled into a single file
        print('confrontational_hatespeech_incitement')
        out=[['platform', 'month', 'term', 'no_items']]
        woink={}
        self.connect()
        mapper = self.readMapper()
        ct=0
        out = [['platform', 'date', 'keyword', 'language', 'partisanship', 'frequency']]
        self.cur.execute("select date, platform, language, month, id from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        i=0
        while itmr:
            itm = dict(zip(desc, itmr))
            i+=1
 
            if i % 10000 == True:
                print(i/1000)
            try:
                mapper[itm['id']]
            except KeyError:
                itmr=self.cur.fetchone()
                continue

            donethemes = []
            for theme, rows in self.all_themes.items():
                if not theme in wanted_cats:
                    continue
                for row in rows:
                    term = self.makeWordOrIssue(row)

                    word = "%s %s" % (row['Word, hashtag or issue'].lower(), row['AND'].lower())
                    word = word.lower().strip()
                    
                    key = (itm['platform'], itm['month'], word, itm['language'], row['Partisanship'])
                    if key in donethemes:
                        continue
                    word = row['Word, hashtag or issue'].strip().lower()
                    if term in mapper[itm['id']]:
                        try:
                            woink[key] += 1
                        except KeyError:
                            woink[key] = 1
                        donethemes.append(key)
            itmr= self.cur.fetchone()
        
        for k,v in woink.items():
            out.append(list(k) + [v])
        namepart = "_".join(wanted_cats)
        self.writecsv("%s/%s.csv"%(  self.outpath, namepart), out)

    def Number_of_posts_per_platform_over_time(self):
        #Ouputs Number_of_posts_per_platform_over_time in a csv
        print('Number_of_posts_per_platform_over_time')
        out=[['date','platform','no_items']]
        woink={}
        for itm in self.data:
            try:
                key = (itm['date'], itm['platform'])
                try:
                    woink[key] += 1
                except KeyError:
                    woink[key] = 1
            except KeyError:
                pass
        for k,v in woink.items():
            out.append(list(k) + [v])
        self.writecsv("%s/Number_of_posts_per_platform_over_time.csv"%self.outpath, out)
    
    def Post_or_video_date_engagement_platform_language(self):
        #see method name
        print('Post_or_video_date_engagement_platform_language')
        out=[['Post_or_video', 'date', 'engagement', 'platform', 'language', 'user', 'usercat']]
        for itm in self.data:
            cat = self.getCat(itm)
            out.append([itm['text'], itm['date'], itm['score'], itm['platform'], itm['language'], itm['user'], cat])
        self.writecsv("%s/Post_or_video_date_engagement_platform_language.csv"%self.outpath, out)

    def What_are_language_groups_talking_about(self):
        #see method name
        print('What_are_language_groups_talking_about')
        out=[['Keyword','date','user', 'theme', 'source','platform','language', 'partisanship', 'engagement', 'frequency']]
        self.connect()
        mapper=self.readMapper()
        mapper2={}
        self.cur.execute("select id, date, usr as user, text, category, platform, score, newscore, language from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        i=0
        woink={}
        engagewoink={}
        while itmr:
            i+=1
            itm = dict(zip(desc, itmr))
            itm['score'] = itm['score']
            if itm['newscore']:
                itm['score'] = itm['newscore']
        
            if i % 1000 == True:
                print(i/1000,'wlgtab')
            try:
                mapper[itm['id']]
            except KeyError:
                itmr=self.cur.fetchone()
                continue
            txt=itm['text'].lower()
            donethemes = []
            for theme, rows in self.all_themes.items():
                for row in rows:
                    term = self.makeWordOrIssue(row)
                    
                    word = row['Word, hashtag or issue'].lower()
                    compound_word = "%s %s" % (row['Word, hashtag or issue'].lower(), row['AND'].lower())
                    compound_word = compound_word.lower().strip()
                    key = (compound_word, itm['date'], itm['user'], theme, itm['category'], 
                        itm['platform'], 
                        itm['language'],
                        row['Partisanship'].lower().strip()
                        )
                    if term in mapper[itm['id']]:
                            try:
                                woink[key] += 1
                            except KeyError:
                                woink[key] = 1
                            if itm['score'] == -1:
                                itm['score'] = 0
                            try:
                                engagewoink[key] += itm['score']
                            except KeyError:
                                engagewoink[key] = itm['score']
            itmr=self.cur.fetchone()
        for k,v in woink.items():
            out.append(list(k) + [engagewoink[k], v])
        self.writecsv("%s/What_are_language_groups_talking_about.csv"%self.outpath, out)
        print(i)

    def getCountsPerLanguage(self):
        #Counts number of posts per language.
        #IIRC this is an updated version of the other method that purports to do the same
        dic = {
            'yt':{},
            'fb':{},
            'tw':{},
            'vk':{},
        }
        for itm in self.data:
            try:
                dic[itm['platform']][itm['language']]+=1
            except KeyError:
                dic[itm['platform']][itm['language']]=1
        for k,v in dic.items():
            print(k)
            for kk, vv in v.items():
                print([k,kk,vv])

    def getCountsPerLanguageOverall(self):
        #see title. Outputs to command line
        dic={}
        for itm in self.data:
            try:
                dic[itm['language']]+=1
            except KeyError:
                dic[itm['language']]=1
        for k,v in dic.items():
            print([k,v])

    def What_are_language_groups_talking_about_per_week(self):
        #deprecated
        print('What_are_language_groups_talking_about_per_week')
        out=[['Keyword','week','user', 'theme', 'user/source','platform','language', 'frequency']]
        woink={}
        for i, itm in enumerate(self.data):
            for word, theme in self.word2theme.items():
                if word in itm['text'].lower():
                    if re.search(r'\b%s\b' % word, itm['text'].lower()):
                        key = (word, itm['week'], itm['user'], theme, itm['category'], itm['platform'], itm['language'])
                        try:
                            woink[key] += 1
                        except KeyError:
                            woink[key] = 1
        for k,v in woink.items():
            out.append(list(k) + [v])
        self.writecsv("%s/What_are_language_groups_talking_about_per_week.csv"%self.outpath, out)

    def To_what_extent_do_we_see_a_surge_of_conciliatory_activities(self):
        #Shows conciliatory posts over time per platform/language
        print('To_what_extent_do_we_see_a_surge_of_conciliatory_activities')
        word2theme={}
        out=[['conciliatory post','week','platform','language']]
        for k,v in self.word2theme.items():
            if v == 'conciliatory':
                word2theme[k]=v
        for i, itm in enumerate(self.data):
            ok=False
            for word, theme in word2theme.items():
                if word in itm['text'].lower():
                    if re.search(r'\b%s\b' % word, itm['text'].lower()):
                        ok=True
                        break
            if ok:
                out.append([itm['text'], itm['week'], itm['platform'], itm['language']])
        self.writecsv("%s/To_what_extent_do_we_see_a_surge_of_conciliatory_activities.csv"%self.outpath, out)

    def postEngagement(self):
        #Spammy method that outputs EVERYTHING and its engagement
        print("postEngagement")
        out= [['date', 'source', 'post (actual thing)', 'platform', 'engagement', 'language']]
        for itm in self.data:
            out.append([itm['date'], itm['category'], itm['text'], itm['platform'], itm['score'], itm['language']])
            
        self.writecsv("%s/postEngagement.csv"%self.outpath, out)
   
    def Number_of_sources_per_platform(self):
        #Counts the number of categories per platform
        print('Number_of_sources_per_platform')
        out=[['platform','category','language','no_items']]
        woink={}
        for itm in self.data:
            # cat = self.getCat(itm)
            try:
                key = (itm['platform'], itm['category'], itm['language'])
                try:
                    woink[key] += 1
                except KeyError:
                    woink[key] = 1
            except KeyError:
                pass
        for k,v in woink.items():
            out.append(list(k) + [v])
        self.writecsv("%s/Number_of_sources_per_platform.csv"%self.outpath, out)

    def find_all_hashtags(self):
        #Outputs hashtags in the data, deprecated. Hashtags matches are now stored in the database as the matching is slow
        hashes=[]
        tot=len(self.data)
        for i, itm in enumerate(self.data):
            if i % 100000 == True:
                print('Find hashes', i, tot)
            qq = re.findall(self.hashtagregex, itm['text'])
            for q in qq:
                hashes.append(q)
        from collections import Counter
        c=Counter(hashes)
        with codecs.open('all_hashtags.csv','w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows([[k,v] for k,v in c.items()])
            
    def words_per_hashtag_per_categorylanguage(self):
        #also deprecated
        out = [['frequent word', 'frequent hashtag','source','language']]
        woink={}
        for i, itm in enumerate(self.data):
            for word, theme in self.word2theme.items():
                if word in itm['text'].lower():
                    if re.search(r'\b%s\b' % word, itm['text'].lower()):
                        hashes=[]
                        qq = re.findall(self.hashtagregex, itm['text'].lower())
                        for q in qq:
                            hashes.append(q)
                        for hash in hashes:
                            key = (word, hash, itm['category'], itm['language'])
                            try:
                                woink[key] += 1
                            except KeyError:
                                woink[key] = 1
        for k,v in woink.items():
            out.append(list(k) + [v])
        self.writecsv("%s/words_per_hashtag_per_categorylanguage.csv"%self.outpath, out)
    
    def PeaceAndConciliatoryHashies(self):
        #Intended to get an overview of peaceful/conciliatory hashtags and their engagement.
        #Now deprecated
        print('PeaceAndConciliatoryHashies')
        pt = self.getPeaceHashtags()
        mapke={}
        pieces = ['#ArmAzeriPeace','#ArmAzetalks','#MakeTolmaNotWar','#peace','#Reconciliation','#NKpeace','#PeaceNotWar']
        pieces = [pp.lower() for pp in pieces]
        for p in pt:
            if p in pieces:
                mapke[p] = 'peace'
            else:
                mapke[p] = 'peace on one\'s own terms'
        for k,v in self.word2theme.items():
            if k.startswith('#') and v == 'conciliatory':
                mapke[k.lower()] = v
        lanmap={}

        for itm in self.data:
            for k,v in mapke.items():
                key = (v, k)
                try:
                    lanmap[key].append(itm['language'])
                    lanmap[key]=list(set(lanmap[key]))
                except KeyError:
                    lanmap[key] = [itm['language']]
        woink={}
        print("Start match...")
        for itm in self.data:
            for k,v in mapke.items():
                if re.search(r'\b%s\b'% k, itm['text'].lower()):
                    lans = ",".join(lanmap[(v,k)])
                    key = (v,k, lans)
                    try:
                        woink[key]+=itm['score']
                    except KeyError:
                        woink[key]=itm['score']
        out = [['type', 'hashtag', 'languages', 'size']]
        for k,v in woink.items():
            out.append(list(k)+[v])
        self.writecsv("%s/PeaceAndConciliatoryHashies.csv"%(self.outpath), out)

    def PeaceAndConciliatoryHashiesForSingleLanguage(self):
        #Intended to get an overview of peaceful/conciliatory hashtags and their engagement per language
        #Now deprecated
        print('PeaceAndConciliatoryHashiesForSingleLanguage')
        pt = self.getPeaceHashtags()
        mapke={}
        pieces = ['#ArmAzeriPeace','#ArmAzetalks','#MakeTolmaNotWar','#peace','#Reconciliation','#NKpeace','#PeaceNotWar']
        pieces = [pp.lower() for pp in pieces]
        for p in pt:
            if p in pieces:
                mapke[p] = 'peace'
            else:
                mapke[p] = 'peace on one\'s own terms'
        for k,v in self.word2theme.items():
            if k.startswith('#') and v == 'conciliatory':
                mapke[k.lower()] = v

        woink={}
        print("Start match...")
        for itm in self.data:
            for k,v in mapke.items():
                if re.search(r'\b%s\b'% k, itm['text'].lower()):
                    key = (v,k, itm['language'])
                    try:
                        woink[key]+=itm['score']
                    except KeyError:
                        woink[key]=itm['score']
        out = [['type', 'hashtag', 'language', 'size']]
        for k,v in woink.items():
            out.append(list(k)+[v])
        self.writecsv("%s/PeaceAndConciliatoryHashiesForSingleLanguage.csv"%(self.outpath), out)

    def findAllHashtags(self, filter=None, fileadd = ''):
        #Outputs hashtags in the data, deprecated. Hashtags matches are now stored in the database as the matching is slow
        #Can use an argument to mark the file
        print('findAllHashtags')
        out=[['hashtag','score','languages']]
        hash={}
        hc={}
        for itm in self.data:
            qq = re.findall(self.hashtagregex, itm['text'].lower())
            for q in qq :
                q=q.lower()
                if filter and not q in filter:
                    continue
                try:
                    hash[q].append(itm['language'])
                except KeyError:
                    hash[q] = [itm['language']]
                try:
                    hc[q]+=itm['score']
                except KeyError:
                    hc[q]=itm['score']
            
        for k,v in hash.items():
            out.append([k,hc[k], ",".join(list(set(v)))])
        self.writecsv("%s/findAllHashtags%s.csv"%(self.outpath, fileadd), out)
       
    def coHashtagNetworkPerLanguage(self):
        #Makes a network of co-occurring hashtags
        import networkx as nx
        from itertools import combinations
        networks={}
        for itm in self.data:
            
            qq = re.findall(self.hashtagregex, itm['text'].lower())
            hashes= list(set(qq))
            try:
                networks[itm['language']]
            except KeyError:
                networks[itm['language']]=nx.Graph()
                print(itm['language'])
            for c in combinations(hashes,2):
                if networks[itm['language']].has_edge(c[0], c[1]):
                    networks[itm['language']][c[0]][c[1]]['weight']+=1
                elif networks[itm['language']].has_edge(c[1], c[0]):
                    networks[itm['language']][c[1]][c[0]]['weight']+=1
                else:
                    networks[itm['language']].add_edge(c[1], c[0], weight=1)
        for k,v in networks.items():
            if k == '?':
                k='_'
            nx.write_gexf(v, '%s/networks/hashtags_conetwork_%s.gexf' % (self.outpath, k))
            
    def hashtagCountsOverTime(self, filter=None, fileadd =''):
        #see method name
        print('hashtagCountsOverTime')
        out=[['hashtag','date','languages','score']]
        hash={}
        hc={}
        tot=len(self.data)
        for i, itm in enumerate(self.data):
            if i % 1000 == True:
                print(i, tot)
            qq = re.findall(self.hashtagregex, itm['text'].lower())
            for q in qq :
                q=q.lower()
                if filter and not q in filter:
                    continue
                key = (q, itm['date'])
                try:
                    if not itm['language'] in hash[key]:
                        hash[key].append(itm['language'])
                except KeyError:
                    hash[key] = [itm['language']]
                try:
                    hc[key]+=itm['score']
                except KeyError:
                    hc[key]=itm['score']
        for k,v in hash.items():
            out.append(list(k) +[ ",".join(list(set(v))), hc[k]])
        self.writecsv("%s/hashtagCountsOverTime%s.csv"% (self.outpath,fileadd), out)

    def languageHashTagsMentions(self):
        #Hashtag mentions per language
        print('languageHashTagsMentions')
        out = [['hashtag','original language that mentions it','other languages that mention it']]
        wants = []
        for k,v in self.word2theme.items():
            if k.strip().startswith('#'):
                wants.append(k.strip())
        wants = set(wants)
        hashlans={}
        for itm in self.data:
            for w in wants:
                if w in itm['text'].lower():
                    if re.search(r'\b%s\b' % w, itm['text'].lower()):
                        try:
                            hashlans[w].append(itm['language'])
                        except KeyError:
                            hashlans[w] = [itm['language']]
            
        for k,v in hashlans.items():
            orig_lan = self.word2lan[k]
            others = [a for a in set(v) if not a == orig_lan]
            out.append([k, orig_lan, ','.join(others)])
        self.writecsv("%s/languageHashTagsMentions.csv"%self.outpath, out)
    
    def makeWordOrIssue(self, row):
        #Joins required words and optional words into a single term, convenience method
        qq = ("%s AND %s" % (row['Word, hashtag or issue'], row['AND'])).strip()
        return qq

    def Insertmatches(self):
        #Inserts matches of hashtags with posts, so we don't have to regexmatch everything every time
        print('Insertmatches')
        
        self.connect()
        self.connect2()
        self.cur.execute("truncate words2posts")
        self.conn.commit()
        self.cur.execute("select id, text from posts")
        # desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        i=0
        toup={}
        while itmr:
        # for itm in self.data:
            i+=1
            # itm = dict(zip(desc, itmr))
            if i % 1000 == True:
                print(i/1000,'IM')
            txt=itmr[1].lower()
            for theme, rows in self.all_themes.items():
                for row in rows:
                    word = row['Word, hashtag or issue'].strip().lower()
                    if word in txt.lower():
                        if row['AND'].strip():
                            if not word in txt or not re.search(r'\b%s\b' % row['AND'].strip().lower(), txt):
                                continue
                        if re.search(r'\b%s\b' % word, txt):
                            qq = self.makeWordOrIssue(row)
                            try:
                                toup[itmr[0]].append(qq)
                            except KeyError:
                                toup[itmr[0]] = [qq]
            if len(toup) > 100:
                for k,v in toup.items():
                    for vv in v:
                        print([k,vv])
                        self.cur2.execute("insert into words2posts (postid, word) values (%(i)s, %(w)s)", {
                            'w': vv, 'i': k
                        })
                toup={}
                print("UPDATE")
                self.conn2.commit()
            itmr=self.cur.fetchone()
        ct=0
        for k,v in toup.items():
            for vv in v:

                print([k,vv])
                self.cur.execute("insert into words2posts (postid, word) values (%(i)s, %(w)s)", {
                    'w': vv, 'i': k
                })
            ct+=1
            if ct % 100 == True:
                self.conn.commit()
        self.conn.commit()

    def readMapper(self):
        #Method to grab the posts that mention relevant words from the .csv
        self.connect()
        self.cur.execute("select postid, word from words2posts")
        row=self.cur.fetchone()
        mapper = {}
        ct=0
        while row:
            ct+=1
            if ct % 1000 == True:
                print("RM",ct)
            try:
                mapper[row[0]].append(row[1])
            except KeyError:
                mapper[row[0]] = [row[1]]
            row = self.cur.fetchone()
        return mapper

    def Number_of_posts_per_theme_over_time(self):
        #see method name
        print('Number_of_posts_per_theme_over_time')
        out=[['date','platform','language','partisanship','theme','no_items']]
        woink={}

        self.connect()

        mapper = self.readMapper()
        ct=0
        print("QUERY")
        self.cur.execute("select text, platform, date, language, id from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        i=0
        while itmr:
            i+=1
            itm = dict(zip(desc, itmr))
            if i % 10000 == True:
                print(i/1000,'NPpT')
            try:
                mapper[itm['id']]
            except KeyError:
                itmr=self.cur.fetchone()
                continue
            txt=itm['text'].lower()
            donethemes = []
            for theme, rows in self.all_themes.items():
                for row in rows:
                    key = (itm['date'], itm['platform'], itm['language'], row['Partisanship'], theme)
                    if key in donethemes:
                        continue
                    term = self.makeWordOrIssue(row)
                    if term in mapper[itm['id']]:
                            try:
                                woink[key] += 1
                            except KeyError:
                                woink[key] = 1
                            donethemes.append(key)
            itmr = self.cur.fetchone()
        for k,v in woink.items():
            out.append(list(k) + [v])
        self.writecsv("%s/Number_of_posts_per_theme_over_time.csv"%self.outpath, out)

    def Most_engaged_with_content_per_platform_per_language(self):
        #see method name
        print('Most_engaged_with_content_per_platform_per_language')
        out=[['user', 'source', 'word','date','platform','language','tag','score']]
        woink={}
        rolled=0
        for itm in self.data:
            if itm['score'] < 0:#no use including stuff without metrics
                continue
            rolled+=1
            tag = self.getCat(itm)
            if tag:
                for word, theme in self.word2theme.items():
                    if word in itm['text'].lower():
                        ct = len(re.findall(r'\b%s\b' % word, itm['text'].lower()))
                        if ct > 0:
                            lan = self.word2lan[word]
                            try:
                                key = (itm['user'], row['source'], word, itm['date'], itm['platform'], itm['language'], tag)
                                try:
                                    woink[key] += itm['score']
                                except KeyError:
                                    woink[key] = itm['score']
                            except KeyError:
                                pass
        for k,v in woink.items():
            out.append(list(k) + [v])
        print(len(out), rolled)
        self.writecsv("%s/Most_engaged_with_content_per_platform_per_language.csv"%self.outpath, out)
        
    def Most_engaged_with_content_per_language(self):
        #shows most engaged with content per category per language over time
        print('Most_engaged_with_content_per_platform_per_language')
        out=[['id', 'url', 'source', 'text', 'date', 'language', 'score']]
        out=[[ 'score', 'date', 'language', 'platform', 'text']]
        woink={}
        self.connect()
        
        self.cur.execute("select date, url, text, month, category, platform, score, newscore, language, message_id as id from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        i=0
        
        while itmr:
            itm = dict(zip(desc, itmr))
            itm['score'] = itm['score']
            if itm['newscore']:
                itm['score'] = itm['newscore']
            if not itm['language'] in ['ru', 'en', 'az', 'arm']:
                itm['language'] = 'other'
            i+=1
            if i % 10000 == True:
                print(i, 'Most_engaged_with_content_per_language')
            key = (itm['id'], itm['url'], itm['category'], itm['text'], itm['date'], itm['language'])
            try:
                woink[itm['platform']]
            except KeyError:
                woink[itm['platform']] = {}
            try:
                woink[itm['platform']][itm['language']]
            except KeyError:
                woink[itm['platform']][itm['language']] = {}
      
            try:
                woink[itm['platform']][itm['language']][itm['month']]
            except KeyError:
                woink[itm['platform']][itm['language']][itm['month']] = {}
            try:
                woink[itm['platform']][itm['language']][itm['month']][itm['score']].append(itm['text'])
            except KeyError:
                woink[itm['platform']][itm['language']][itm['month']][itm['score']]=[itm['text']]
            itmr = self.cur.fetchone()
        for platform, v in woink.items():
            for language, vv in v.items():
                for month, vvv in vv.items():
                    act=0
                    for score, texts in sorted(vvv.items(), reverse=True):
                        if act > 20:
                            continue
                        for text in texts:
                            act+=1
                            if act > 20:
                                continue
                            out.append([score, month, language, platform, " ".join(text.split())])
        self.writecsv("%s/Most_engaged_with_content_per_language.csv"%self.outpath, out)
        
    def Conciliatory_discourses_specific_keywords(self):
        #Ouputs csv with frequencies over time of conciliatory and historical issues
        print('Conciliatory_discourses_specific_keywords')
        self.connect()
        mapper=self.readMapper()
        ct=0
        woink={}
        self.cur.execute("select id, language, date from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        i=0
        while itmr:
            i+=1
            itm = dict(zip(desc, itmr))
            
            if i % 1000 == True:
                print(i/1000, 'CDSK')
            try:
                mapper[itm['id']]
            except KeyError:
                itmr=self.cur.fetchone()
                continue
            donethemes = []
            for theme, rows in self.all_themes.items():
                if not theme in ['conciliatory', 'historical issue']:
                    continue
                for row in rows:
                    word = row['Word, hashtag or issue'].strip().lower()
                    compound_word = "%s %s"% (word, row['AND'].lower())
                    compound_word = compound_word.strip()
                    term = self.makeWordOrIssue(row)

                    key = (itm['date'], compound_word, itm['language'], row['Partisanship'], theme)
                    if key in donethemes:
                        continue
                    if term in mapper[itm['id']]:
                            try:
                                woink[key] += 1
                            except KeyError:
                                woink[key] = 1
                            donethemes.append(key)
            itmr = self.cur.fetchone()
        out=[['date','word','language','partisanship','theme','frequency']]
        for k,v in woink.items():
            out.append(list(k) + [v])
        self.writecsv("%s/Conciliatory_discourses_specific_keywords.csv"%self.outpath, out)
        
    def filter_Most_frequent_keywords_queries_per_platform(self):
        #As method name suggests
        from operator import itemgetter
        freqspt={}
        with codecs.open("%s/Most_frequent_keywords_queries_per_platform.csv"%self.outpath,'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                language= row['term']
                partisanship= row['language']
                term= row['partisanship']
                row['term'] = term
                row['language'] = language
                row['partisanship'] = partisanship
                try:
                    freqspt[row['partisanship']]
                except KeyError:
                    freqspt[row['partisanship']]={}
                try:
                    freqspt[row['partisanship']][row['term']]+= int(row['no_items'])
                except KeyError:
                    freqspt[row['partisanship']][row['term']] = int(row['no_items'])
        oks=[]
        for t, v in freqspt.items():
            for kk,vv in sorted(v.items(), reverse=True, key=itemgetter(1))[:5]:
                oks.append(kk)
                print(kk,vv)
        oks = set(oks)
        out=[]
        with codecs.open("%s/Most_frequent_keywords_queries_per_platform.csv"%self.outpath,'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                language= row['term']
                partisanship= row['language']
                term= row['partisanship']
                row['term'] = term
                row['language'] = language
                row['partisanship'] = partisanship
                if row['term'] in oks:
                    out.append(row)
        with codecs.open("%s/Most_frequent_keywords_queries_per_platform_filtered.csv"%self.outpath, 'w', encoding='utf-8') as f:
            w=csv.DictWriter(f, delimiter=';', fieldnames=list(row.keys()))
            w.writeheader()
            w.writerows(out)

    def filter_Conciliatory_discourses_specific_keywords(self):
        #Filters Conciliatory_discourses_specific_keywords by most frequent overall and per-theme items
        from operator import itemgetter
        freqs={}
        freqspt={}
        with codecs.open("%s/Conciliatory_discourses_specific_keywords.csv"%self.outpath,'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                try:
                    freqs[row['word']]+= int(row['frequency'])
                except KeyError:
                    freqs[row['word']] = int(row['frequency'])
                try:
                    freqspt[row['theme']]
                except KeyError:
                    freqspt[row['theme']]={}
                try:
                    freqspt[row['theme']][row['word']]+= int(row['frequency'])
                except KeyError:
                    freqspt[row['theme']][row['word']] = int(row['frequency'])
        oks=[]
        for k,v in sorted(freqs.items(), reverse=True, key=itemgetter(1))[:5]:
            print(k,v)
            oks.append(k)
        for t, v in freqspt.items():
            for kk,vv in sorted(v.items(), reverse=True, key=itemgetter(1))[:5]:
                oks.append(kk)
                print(kk,vv)
        oks = set(oks)
        out=[]
        with codecs.open("%s/Conciliatory_discourses_specific_keywords.csv"%self.outpath,'r', encoding='utf-8') as f:
            r=csv.DictReader(f, delimiter=';')
            for row in r:
                if row['word'] in oks:
                    out.append(row)
        with codecs.open("%s/Conciliatory_discourses_specific_keywords_filtered.csv"%self.outpath, 'w', encoding='utf-8') as f:
            w=csv.DictWriter(f, delimiter=';', fieldnames=list(row.keys()))
            w.writeheader()
            w.writerows(out)

    def getText(self, dic):
        #Grabs the meat of the post (text, body etc) from the various datasets
        text=None
        if 'body' in dic.keys():
            dic['text'] = dic['body']
        try:
            text = "%s %s"  % (dic['title'], dic['text'])
        except KeyError:
            try:
                text = dic['text']
            except KeyError:
                try:
                    text = dic['Message']
                except KeyError:
                    try:
                        text = "%s %s" % (dic['videoTitle'], dic['videoDescription'])
                    except KeyError:
                        print("TEXT ERR", dic)
                        sys.exit('!!!')
        return text.lower().strip()
 
    def getLanguage(self, dic, text, platform, usr, message_id):
        #this one is unnecessary
        return self.detect_lang(text)

    def saveData(self, path='d:/nagornodata.p.gz'):
        #Saves the current dataset to disk
        import gzip
        with gzip.open(path,'wb') as f:
            pickle.dump(self.data, f)

    def loadData(self):
        #Loads the dataset from disk
        self.connect()
        self.data = []
        print("Load...")
        if self.test:
            self.cur.execute("select * from posts limit 10")
        else:
            self.cur.execute("select * from posts")
        row = self.cur.fetchone()
        desc = [a[0] for a in self.cur.description]
        ct=0
        while row:
            ct+=1
            if ct % 10000 == True:
                print(ct/10000, 'L 4770.001')
            rrow=dict(zip(desc,row))
            rrow['id'] = rrow['message_id']
            rrow['important_issue'] = eval(rrow['important_issue'])
            if rrow['newscore']:
                rrow['score'] = rrow['newscore']
            del rrow['message_id']
            self.data.append(rrow)
            row = self.cur.fetchone()

    def getUser(self, dic):
        #Gets the user id/name from the various fields in the various datasets
        usr=None
        try:
            usr = dic['author']
        except KeyError:
            try:
                usr = dic['from_id']
            except KeyError:
                try:
                    usr = dic['channelTitle']
                except KeyError:
                    try:
                        usr = dic['author_id']
                    except KeyError:
                    
                        if 'Group Name' in dic.keys():
                            usr = dic['Group Name']
                        elif '\ufeffGroup Name' in dic.keys():
                            usr = dic['\ufeffGroup Name']
                        else:
                            try:
                                usr = dic['Page Name']
                            except KeyError:
                                try:
                                    usr = dic['\ufeffPage Name']
                                except KeyError:
                                    print(dic.keys())
                                    sys.exit("FB USER ERR")
                        if not usr:
                            try:
                                usr = dic['User Name']
                            except KeyError:
                                print(dic.keys())

                                print("USER ERR")#, dic)
                                sys.exit('!!!')
            return usr

    def getCat(self, dic):
        #Gets the category from this data item
        cat=''
        try:
            cat = dic['source']
        except KeyError:
            try:
                cat = dic['Page Category']
            except KeyError:
                try:
                    cat = dic['videoCategoryLabel']
                except KeyError:
                    pass
        if cat is None:
            cat = ''
        try:
            cat = self.categorisations[cat.lower().strip()]
        except KeyError:
            pass
        cat = self.category_override(cat.lower().strip())
        return cat

    def getScore(self, dic, platform):
        #Gets the 'engagement score' for this item. These vary wildly between, and even within, platforms
        score=0
        if platform == 'tl':
            try:
                score = int(dic['views'])
            except ValueError:
                score=0
        elif platform == 'vk':
            if 'likes' in dic.keys():
            # if not 'views' in dic.keys():
                score += int(eval(dic['likes'])['count'])
            else:
                for x in ['views']:
                    if dic[x] and not dic[x] == '':
                        # print(dic[x])
                        try:
                            score += int(dic[x]['count'])
                        except TypeError:
                            score += int(eval(dic[x])['count'])
        elif platform == 'tw':
            try:
                try:
                    dic['public_metrics'] = self.extratwittermetrics[str(dic['id'])]
                except KeyError:
                    pass
                for x in ['retweet_count', 'reply_count', 'like_count', 'quote_count']:
                    try:
                        if dic['public_metrics'][x] and not dic['public_metrics'][x] == '':
                            score += int(dic['public_metrics'][x])
                    except Exception:
                        try:
                            score += int(dic[x])
                        except KeyError:
                            score = -1
            except TypeError as err:
                print(err)
                print(dic)
                print("getScore error!")
                sys.exit()
        elif platform == 'fb':
            try:
                try:
                    score = int(dic['Total Interactions (weighted  —  Likes 1x Shares 1x Comments 1x Love 1x Wow 1x Haha 1x Sad 1x Angry 1x Care 1x )'].replace(',',''))
                except KeyError:
                    score = int(dic['Interaction Rate (weighted  —  Likes 1x Shares 1x Comments 1x Love 1x Wow 1x Haha 1x Sad 1x Angry 1x Care 1x )'].replace(',','').split('.')[0])
            except ValueError:
                score=-1
        elif platform == 'yt':
            for x in ['viewCount', 'likeCount', 'commentCount', 'dislikeCount', 'favoriteCount']:
                if dic[x] and not dic[x] == '':
                    
                    try:
                        score += int(dic[x].split('.')[0])
                    except ValueError:
                        score=-1
        else:
            sys.exit("Unknown platform!!")
        return score

    def getImportantIssue(self, dic, text):
        #Gets all Important Issues
        issues=[]
        for word, issue in self.word2important.items():
            if word in text.lower():
                ct = len(re.findall(r'\b%s\b' % word, text.lower()))
                if ct > 0:
                    issues.append((word, issue))
        return issues

    def Frequency_of_issues_important_to_each_language_group_mentioned_by_each_language_group(self):
        #as name suggests, deprecated atm
        out = [['week', 'important issue', 'to whom? (Ar/Az)', 'mentioned by whom? (Az/Ar)', 'platform', 'frequency']]
        tmpdata=[]
        for itm in self.data:
            if not itm['important_issue'] == []:
                important_issue = ",".join([w[0] for w in itm['important_issue']])
                important_issue_to = ",".join([w[1] for w in itm['important_issue']])
                if important_issue == ',' or important_issue_to == ',':
                    continue
                tmpdata.append((itm['week'], important_issue, important_issue_to, itm['language'], itm['platform']))
        counted = Counter(tmpdata)
        for k,v in counted.items():
            out.append(list(k) + [v])
        self.writecsv('%s/Frequency_of_issues_important_to_each_language_group_mentioned_by_each_language_group.csv' % self.outpath, out)
        
    def sortTextPerMonth(self):
        #Sorts the data into month-sized chunks
        permonth = {}
        self.connect()
        self.cur.execute("select month, text from posts")
        row=self.cur.fetchone()
        self.data = []
        while row:
            try:
                permonth[row[0]].append(row[1])
            except KeyError:
                permonth[row[0]] = [row[1]]
            row=self.cur.fetchone()
        return permonth

    def category_override(self, cat_user_tuple):
        #Override categories with Davit's categorisation
        overrides = {
            'Karabakh is Azerbaijan!'.lower(): 'activist',
            'Azad Bitərəf Gənclər Birliyi'.lower(): 'activist',
            'VƏTƏN ÖVLADLARI'.lower(): 'activist',
            'Əsir Yurdlarımıza Azadlıq!'.lower(): 'activist',
            'Qarabağ Azərbaycanındır'.lower(): 'activist',
            'Azerbaijan social media Army✊🇦🇿'.lower(): 'activist',
            'Freedom for Mubariz Mensimov!'.lower(): 'activist',
            'SÖZ AZADLIĞI GRUPU.🇦🇿'.lower(): 'activist',
            'Qarabağ Azərbaycandır!!!'.lower(): 'activist',
            'BIZIM AZERBAYCAN'.lower(): 'activist',
            '"Prezident və Vitse-Prezidentə İCTİMAİ DƏSTƏK"'.lower(): 'activist',
            'Güclü və qüdrətli Azərbaycan naminə İlhamla Mehribanla irəli!'.lower(): 'activist',
            'ŞAMAXI FƏALLARI !'.lower(): 'activist',
            'ŞƏHİDLƏR UNUTMAYAQ UNUTDURMAYAQ'.lower(): 'activist',
            'Vətən Naminə Birlik Partiyasinin Sebail Rayonu Teşkilati.'.lower(): 'activist',
            'VƏTƏNİM AZƏRBAYCAN'.lower(): 'activist',
            'YADDAN ÇIXMAZ QARABAĞ...'.lower(): 'activist',
            '🇦🇿QARABAĞ🇦🇿 QAZİLƏRİ🇦🇿'.lower(): 'activist',
            'Vətən aşiqləri!'.lower(): 'activist',
            '🇦🇿QARABAĞ QAZİLƏRİMİZ🇦🇿'.lower(): 'activist',
            '🇦🇿AYƏT və HƏDİSLƏR🇦🇿'.lower(): 'activist',
            '🏴 ƏHLİBEYT NÖKƏRLƏRİ 🇦🇿 !!!'.lower(): 'activist',
            'Ədalət Üçün Səsini Yüksəlt'.lower(): 'activist',
            'PREZİDENTƏ DƏSTƏK VERƏNLƏR'.lower(): 'activist',
            '🇦🇿GÜCLÜ LİDER - ÜMÜD YOLU🇦🇿'.lower(): 'activist',
            'ONLY MOTO 🇦🇿'.lower(): 'activist',
            'Azərbaycan Dövlət Aqrar Universiteti tələbələrindən Cənab Prezidentə dəstək'.lower(): 'activist',
            'QARABAG AZƏRBAYCANINDIR!'.lower(): 'activist',
            'İCTİMAİ TV'.lower(): 'media',
            'Azərbaycan Respublikası Müdafiə Nazirliyi'.lower(): 'gov-non-gov',
            'Diasporla İş üzrə Dövlət Komitəsi'.lower(): 'gov-non-gov',
            'AZERBAIJANI MILITARY PORTAL'.lower(): 'gov-non-gov',
            'Azərbaycan Respublikası Dövlət Gömrük Komitəsi'.lower(): 'gov-non-gov',
            'Azərbaycan Prokurorluğu'.lower(): 'gov-non-gov',
            'Azərbaycan Respublikasının Əmək və Əhalinin Sosial Müdafiəsi Nazirliyi'.lower(): 'gov-non-gov',
            'Ekologiya və Təbii Sərvətlər Nazirliyi'.lower(): 'gov-non-gov',
            'Azərbaycan Respublikasının Daxili İşlər Nazirliyi'.lower(): 'gov-non-gov',
            'Daşkəsən rayon Heydər Əliyev Mərkəzi'.lower(): 'gov-non-gov',
            'Saatlı Heydər Əliyev Mərkəzi'.lower(): 'gov-non-gov',
            'Entertainment'.lower(): 'entertanement',
            'Entertainment'.lower(): 'entertanement',
            'MALFO'.lower(): 'entertament',
            'HÜSÜ PUBGM'.lower(): 'entertament',
            'Cefer Memmedov'.lower(): 'entertament',
            'OVER GAMER İDRİS'.lower(): 'entertament',
            'Babek Tahirov'.lower(): 'entertament',
            'AMATRI STYLE'.lower(): 'entertament',
            'Pubg Dizzy'.lower(): 'entertament',
            'Tema Oyun'.lower(): 'entertament',
            'Ali Maharramov'.lower(): 'entertament',
            'Ali Bəy'.lower(): 'entertament',
            'MO GAMES'.lower(): 'entertament',
            'Esme'.lower(): 'entertament',
            'Video Dunyasi'.lower(): 'entertament',
            'Tohud'.lower(): 'entertament',
            'Yusif Visali'.lower(): 'entertament',
            'ELminn'.lower(): 'entertament',
            'Dwep Game'.lower(): 'entertament',
            'QaRma QaRışıq'.lower(): 'entertament',
            'Shako'.lower(): 'entertament',
            'NATO TİTO'.lower(): 'entertament',
            '44 Ded'.lower(): 'entertament',
            'Qafqaz Gamer'.lower(): 'entertament',
            'ASEF BERDELI'.lower(): 'entertament',
            'Kravital'.lower(): 'entertament',
            'Şeyx'.lower(): 'entertament',
            'ISLAM ASHRAFOV'.lower(): 'entertament',
            'TangledPubg'.lower(): 'entertament',
            'Bayram Kərimli Resmi'.lower(): 'entertament',
            'LOBİN'.lower(): 'entertament',
            'SemedEzimov'.lower(): 'entertament',
            'Ganiyev Game'.lower(): 'entertament',
            'Özet Tv'.lower(): 'entertament',
            'Heydər Pro'.lower(): 'entertament',
            'Youtube vidio'.lower(): 'entertament',
            'LORD AZAD'.lower(): 'entertament',
            'İbadzade PUBG'.lower(): 'entertament',
            'Alien PUBG'.lower(): 'entertament',
            'HAYALET OYUNCU'.lower(): 'entertament',
            'XAN'.lower(): 'entertament',
            'MR FATAL'.lower(): 'entertament',
            'ToTo'.lower(): 'entertament',
            'Tapdıq Abi'.lower(): 'entertament',
            'VUGAR PUBG'.lower(): 'entertament',
            'KabusYT'.lower(): 'entertament',
            'MARYqueen PUBG'.lower(): 'entertament',
            'Crev'.lower(): 'entertament',
            'WØSDER'.lower(): 'entertament',
            'eSports Azerbaijan'.lower(): 'entertament',
            'ODN TV'.lower(): 'entertament',
            'Pubg AzE'.lower(): 'entertament',
            'MURAD YT'.lower(): 'entertament',
            'Qara Maska'.lower(): 'entertament',
            'Eldar Kerimov'.lower(): 'entertament',
            'CHARLY GAMING'.lower(): 'entertament',
            'Turkish kaos'.lower(): 'entertament',
            'MEHİ GAMER VLOG'.lower(): 'entertament',
            'SsalmanN - BS'.lower(): 'entertament',
            'Jin'.lower(): 'entertament',
            'TFC SEZAR'.lower(): 'entertament',
            'Hüseyn Bagirzade'.lower(): 'entertament',
            'WIZ Captain YT'.lower(): 'entertament',
            'Ümid Sadıxov'.lower(): 'entertament',
            'Xaos Pasa'.lower(): 'entertament',
            'Rusiya Streaming'.lower(): 'entertament',
            'Hacı Əliyev'.lower(): 'entertament',
            'Muhammed Official'.lower(): 'entertament',
            'Dead Team'.lower(): 'entertament',
            'ARDOUR'.lower(): 'entertament',
            'SALİH EMRE KAYA'.lower(): 'entertament',
            'Nweezy'.lower(): 'entertament',
            'AE TURK-AZE'.lower(): 'entertament',
            'AzerFreedomTV QARABAĞ'.lower(): 'activist',
            'İNQİLAB TV!'.lower(): 'activist',
            'AzerFreedomTV BIRLIK'.lower(): 'activist',
            'Həqiqət axtarışında'.lower(): 'activist',
            'AzerFreedomTV AĞDAM'.lower(): 'activist',
            'Vətənimin Nəbzi'.lower(): 'activist',
            'AZƏRBAYCAN TARİXİ'.lower(): 'activist',
            'HƏRB TV'.lower(): 'activist',
            'Karabakh is Azerbaijan'.lower(): 'activist',
            'Patriots of Azerbaijan'.lower(): 'activist',
            'BİZİM VƏTƏN'.lower(): 'activist',
            'Azerfreedom TV Almaniya'.lower(): 'activist',
            'Milli Fədailər Hərəkatı'.lower(): 'activist',
            'AZERFREEDOM TV Niyal'.lower(): 'activist',
            'AzerFreedom GEORGIA'.lower(): 'activist',
            'hərbi xəbərlər'.lower(): 'activist',
            'YAŞASIN AZƏRBAYCAN'.lower(): 'activist',
            'AzerFreedomTV Namiq'.lower(): 'activist',
            'AzerFreedom TV BƏRDƏ'.lower(): 'activist',
            'VƏTƏNİM AZƏRBAYCAN. RGNMNAF'.lower(): 'activist',
            'TƏHSİL XƏBƏRLƏRİ FORUMU Babək Rəcəbli'.lower(): 'activist',
            'AzerFreedomTV AĞDAM'.lower(): 'activist',
            'AzerFreedom TV'.lower(): 'activist',
            'Azerbaijan Cyber Army'.lower(): 'activist',
            'Azərbaycan Tarixi'.lower(): 'activist',
            'AzerFreedom TV Azadlıq'.lower(): 'activist',
            'Vetenim Azerbaycan'.lower(): 'activist',
            'Qarabağ Azərbaycandır'.lower(): 'activist',
            'Qarabağ Azərbaycandır!'.lower(): 'activist',
            'MƏHƏMMƏD İSMAYILZADƏ ŞƏHİD'.lower(): 'activist',
            'QARABAĞ AZƏRBAYCANDIR!!!'.lower(): 'activist',
            'ÇİNGİZ MUSTAFAYEV MiLLi QƏHRAMAN'.lower(): 'activist',
            'QARABAĞ AZƏRBAYCANDIR'.lower(): 'activist',
            'КАРАБАХ АЗЕРБАЙДЖАН'.lower(): 'activist',
            'Çiçəklənən Azərbaycan'.lower(): 'activist',
            'Military Girl'.lower(): 'activist',
            'Հայ զինվոր'.lower(): 'activist',
            'Հայ Զինվոր'.lower(): 'activist',
            'ArmFreeVoiceNews'.lower(): 'activist',
            'Карабах – Азербайджан!'.lower(): 'activist',
            'КАРАБАХ АЗЕРБАЙДЖАН'.lower(): 'activist',
            'ԶԻՆՈՒԺ MEDIA'.lower(): 'activist',
            'Հայոց Ազգային Բանակ'.lower(): 'gov-non-gov',
            'Armenian National Music'.lower(): 'gov-non-gov',
            'Armenian National Network'.lower(): 'gov-non-gov',
            'ԱՀ ՊԱՇՏՊԱՆՈւԹՅԱՆ ԲԱՆԱԿ NKR DEFENSE ARMY'.lower(): 'gov-non-gov',
            'Azərbaycan Respublikası Müdafiə Nazirliyi'.lower(): 'gov-non-gov',
            'Ministry of Foreign Affairs of Armenia'.lower(): 'gov-non-gov',
            'Diasporla İş üzrə Dövlət Komitəsi'.lower(): 'gov-non-gov',
            '"АРЦАХ". Время признаний.'.lower(): 'activist',
			'✠♰❃ Armenian awakening 🇦🇲'.lower(): 'activist',
			'Alliance for Liberty and Justice in Armenia'.lower(): 'activist',
			'Հայկական Վերածնունդ Սփյուռքյան Շարժում | Armenian Renaissance Movement'.lower(): 'activist',
			'Հայոց Ազգային Բանակ'.lower(): 'activist',
			'Armenian Pride - Armenian Friends'.lower(): 'activist',
			'Հայ զինվոր'.lower(): 'activist',
			'Հայ զինվոր'.lower(): 'activist',
			'ARMY OF LIGHT'.lower(): 'activist',
			'Ազատ Հայաստան'.lower(): 'activist',
			'Ռազմական Տեղեկագիր Военное Обозрение'.lower(): 'activist',
			'Արծրուն Հովհաննիսյան'.lower(): 'activist',
			'Մայր Հայաստան'.lower(): 'activist',
			'🇦🇲ԱՄԵՆ ԻՆՉ ԼԱՎԱ ԼԻՆԵԼՈՒ։ ԿԵՑՑԵ ՀԱՅԱՍՏԱՆԻ ՀԱՆՐԱՊԵՏՈՒԹՅՈՒՆԸ🇦🇲'.lower(): 'activist',
			'Armenian Embassy'.lower(): 'gov-non-gov',
			'Embajada de Armenia en Argentina'.lower(): 'gov-non-gov',
			'Armenian National Association'.lower(): 'gov-non-gov',
			'ՀՀ կրթության'.lower(): 'gov-non-gov'
			
        }
        if type(cat_user_tuple) == tuple:
            try:
                return overrides[cat_user_tuple[1]]
            except KeyError:
                pass
        else:
            try:
                return overrides[cat_user_tuple]
            except KeyError:
                return cat_user_tuple
        return cat_user_tuple[0]

    def getYTCatsAndUserNames(self):
        #Counts categorisations per user for YouTube
        az=[]
        heads = ['category','frequency']
        az_out=[heads]
        hy=[]
        hy_out=[heads]
        self.connect()
        self.cur.execute("select category, language from posts where platform = 'yt'")
        row=self.cur.fetchone()
        i=0
        
        while row:
            i+=1
            cat = row[0]
            lan = row[1]
            if lan == 'arm':
                hy.append(cat)
            elif lan == 'az':
                az.append(cat)
            row = self.cur.fetchone()

        count_az = Counter(az)
        print(count_az)
        for k,v in count_az.items():
            if k:
                try:
                    cat = self.categorisations[k.lower().strip()]
                except KeyError:
                    cat=None
            else:
                cat=None
            cat = self.category_override(k)
            az_out.append([k, v,cat])
        self.writecsv('%s/az_usr_cat_count_YT.csv'% self.outpath, az_out)
        count_hy = Counter(hy)
        for k,v in count_hy.items():
            if k:
                try:
                    cat = self.categorisations[k.lower().strip()]
                except KeyError:
                    cat=None
            else:
                cat=None
            cat = self.category_override(k)
            hy_out.append([k,v,cat])
        self.writecsv('%s/hy_usr_cat_count_YT.csv' % (self.outpath), hy_out)
        
    def getFBCatsAndUserNames(self):
        #Counts categorisations per user for YouTube
        az=[]
        heads = ['category','frequency','davit_category']
        az_out=[heads]
        hy=[]
        hy_out=[heads]
        self.connect()
        self.cur.execute("select category, language from posts where platform = 'fb'")
        row=self.cur.fetchone()
        i=0
        
        while row:
            i+=1
            cat = row[0]
            lan = row[1]
            if lan == 'arm':
                hy.append(cat)
            elif lan == 'az':
                az.append(cat)
            row = self.cur.fetchone()
        count_az = Counter(az)
        for k,v in count_az.items():
            try:
                cat = self.categorisations[k.lower().strip()]
            except KeyError:
                cat=None
            cat = self.category_override(k)
            az_out.append([k,v,cat])
        self.writecsv('%s/az_usr_cat_count_FB.csv' % self.outpath,az_out)
        count_hy = Counter(hy)
        for k,v in count_hy.items():
            try:
                cat = self.categorisations[k.lower().strip()]
            except KeyError:
                cat=None
            cat = self.category_override(k)
            hy_out.append([k,v,cat])
        self.writecsv('%s/hy_usr_cat_count_FB.csv' % self.outpath,hy_out)
    
    def writecsv(self, name, data):
        #Convenience method to output csv
        with codecs.open(name,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(data)
        
    def Words_frequently_associated_to_X_in_each_language_over_timev2(self, word, no=30):
        #As name suggests
        from itertools import zip_longest
        from gensim.utils import simple_preprocess
        outfile = "%s/bigram_rankflow_%s.csv" % (self.outpath,word)
        word = word.lower()
        newword = word.replace(' ','_')
        out=[]
        for month, data in sorted(self.sortTextPerMonth().items()):
            wrds=[]
            l1=[month]
            l2=["%s_val"%month]
            bis=[]
            for d in data:
                txt = d.lower().replace(word, newword)
                if newword in d.lower():
                    
                    toks = simple_preprocess(txt)
                    for i, t in enumerate(toks):
                        if t == newword:
                            try:
                                bis.append(toks[i-1])
                            except IndexError:
                                pass
                            try:
                                bis.append(toks[i+1])
                            except IndexError:
                                pass
            c = Counter(bis)
            for q in c.most_common(no):
                l1.append(q[0])
                l2.append(q[1])
            out.append(l1)
            out.append(l2)
        with codecs.open(outfile,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(zip_longest(*out))
        
    def Words_frequently_associated_to_X_in_each_language_over_time(self, word, no=30):
        #As name suggests
        from itertools import zip_longest
        from nltk.collocations import BigramCollocationFinder, BigramAssocMeasures
        from gensim.utils import simple_preprocess
        bigram_measures = BigramAssocMeasures()

        outfile = "%s/bigram_rankflow_%s_samepost.csv" % (self.outpath,word)
        creature_filter = lambda *w: word in w
        word = word.lower()
        outs={}
        out=[]
        for month, data in sorted(self.sortTextPerMonth().items()):
            wrds=[]
            l1=[month]
            for d in data:
                if word in d.lower():
                    wrds.append(simple_preprocess(d.lower()))

            ## Bigrams
            finder = BigramCollocationFinder.from_documents(wrds)
            finder.apply_freq_filter(3)

            for qq in finder.nbest(bigram_measures.likelihood_ratio, no):
                l1.append(" ".join(qq))
            out.append(l1)
        with codecs.open(outfile,'w', encoding='utf-8') as f:
            w=csv.writer(f, delimiter=';')
            w.writerows(zip_longest(*out))

    def categ_dict(self):
        #Just stores and partly parses the category map
        dic = {
        'community': ['people & blogs',  'personal_blog',  'community',  'blogger',  'scientist',  'public figure',  'athlete',  'person',  'dj',  'author',  'photographer',  'actor',  'video creator',  'movie_writer',  'comedy club',  'personal blog',  'personal website',  'structural engineer',  'ophthalmologist',  'psychotherapist',  'motivational speaker',  'dj',  'author',  'publisher',  'psychologist',  'publisher',  'psychologist',  'nonprofits & activism'],
        'education': ['education',  'college & university',  'educational research center',  'computer_training',  'learning',  'education',  'community college',  'educational consultant',  'tutor/teacher',  'education website'],
        'entertament': ['history museum',  'museum',  'event planner',  'just for fun',  'photography videography',  'entertainment website',  'movie theater',  'movie/television studio',  'dance studio',  'fan page',  'karaoke',  'entertainment',  'film & animation',  'songcomedy',  'orchestra',  'fictional character',  'band',  'festival',  'amusement & theme park',  'comedy',  'sports team',  'artist',  'sports_team',  'musician',  'bands_musicians',  'stadium, arena & sports venue',  'sports promoter',  'sports & recreation venue',  'sports league',  'musician/band',  'sports',  'arts & entertainment',  'performance art theatre',  'literary arts',  'art',  'sports club',  'music chart',  'music',  'sports',  'professional sports team',  'arts & humanities website',  'music video',  'performing arts',  'sports event',  'professional sports league',  'sports & recreation',  'recreation & sports website',  'makeup artist'],
        'gov-non-gov': ['armed forces',  'regional website',  'military base',  'police station',  'state',  'city',  'organization',  'consulate & embassy',  'politician',  'government organization',  'nonprofit organization',  'non-governmental organization (ngo)',  'charity organization',  'government official',  'public & government service',  'political party',  'community organization',  'political organization',  'government website',  'political candidate',  'government building',  'youth organization',  'religious organization'],
        'media': ['broadcasting & media production company',  'news & media website',  'tv channel',  'telecommunication company',  'media/news company',  'magazine',  'newspaper',  'social media agency',  'media',  'media agency',  'journalist',  'comedian',  'tv show',  'news & politics',  'radio station',  'tv network',  'social media company',  'news_site',  'book & magazine distributor'],
        'private-sector': ['automotive manufacturer',  'clothing (brand)',  'beauty, cosmetic & personal care',  'interest',  'cafe',  'food & beverage',  'bakery',  'beauty salon',  'locality',  'gaming video creator',  'veterinarian',  'electronics',  'spa',  'advertising agency',  'gas station',  'apparel & clothing',  'food wholesaler',  'bank',  'website',  'home improvement',  'commercial bank',  'accessories',  'airport',  'camera/photo',  'car rental',  'building materials',  'supermarket',  'hair salon',  'jewelry/watches',  'cars',  'ticket sales',  'brand',  'science & technology',  'pets & animals',  'howto & style',  'topic_doctor',  'game',  'jobs',  'auction house',  'baby goods/kids goods',  'test preparation center',  'app page',  'work_team',  'phone/tablet',  'pizza place',  'gym/physical fitness center',  'medical & health',  'autos & vehicles',  'gaming',  'book',  'movie',  'writer',  'church',  'wine/spirits',  'courthouse',  'furniture',  'real estate',  'carpet cleaner',  'transit system',  'teens & kids website',  'marketing agency',  'home decor',  'medical center',  'general dentist',  'cafeteria',  'book & magazine distributor',  'hotel',  'tour agency',  'eco tour agency',  'tourist information center',  'hospital',  'health/beauty',  'health & wellness website',  'motor vehicle company',  'product/service',  'local business',  'electronics store',  'construction company',  'travel agency',  'financial service',  'community service',  'bookstore',  'energy company',  'shopping mall',  'travel company',  'school',  'fast food restaurant',  'cosmetics store',  "baby & children's clothing store",    'airline company',  'mental health service',  'adult entertainment service',  'company',  'cargo & freight company',  'jewelry & watches store',  'taxi service',  'insurance company',  'georgian restaurant',  'internet company',  'buffet restaurant',  'local service',  'chocolate shop',  'travel & events',  'coffee shop',  'transportation service',  'loan service',  'disability service',  'software company',  'shopping & retail',  'electronics company',  'business service',  'e-commerce website',  'social service',  'local & travel website',  'public service',  'business & economy website',  'language school',  'cleaning service',  'martial arts school',  'water treatment service',  'industrial company',  'family style restaurant',  'retail company',  'middle school',  'travel & transportation',  'medical company',  'not a business',  'bridal shop',  'textile company',  'information technology company',  'medical supply store',  'armenian restaurant',  'airline industry service',  'mobile phone shop',  'shopping service',  'internet service provider',  'beauty supply store']
        }
        cd={}
        for k,v in dic.items():
            for kk in v:
                cd[kk.replace('_','_').replace('/','_')] = k
        self.categorisations = cd

    def getPeaceHashtags(self):
        #Gives peace hashtags
        pt = ['#ArmAzeriPeace','#ArmAzetalks','#MakeTolmaNotWar','#peace','#Reconciliation','#NKpeace','#PeaceNotWar','#ArmenianFascism','#ArmenianLies','#dontbelievearmenia','#FakeArmenianGenocide','#armenianaggression','#ArmenianOccupation','#armenianvandalism','#armenianwarcrimes','#KarabakhisAzerbaijan','#Bashlibeltragedy','#ShushaisAzerbaijan','#LongLiveAzerbaijan','#artsakh','#ArtsakhIsArmenia','#RecognizeArtsakh','#MüzəffərAliBaşKomandan','#QarabağAzərbaycandır','#Shusha','#FreeArmenianHostages','#FreeArmenianPOWs','#PeaceForArmenia','#peaceForArmenians','#SanctionAzerbaijan','#StopAzerbaijaniAggression','#StopAzeriAggression','#AliyevWarCriminal','#ArmeniaAgainstTerrorism','#armeniastrong','#artsakhstrong','#defendarmenia','#dictatorAliyev','#ShushiIsArmenia','#stopaliyev','#UntilTheyAreHome','#haxteluenq','#գյոռբագյոռ','#ծխելով','#ՀԱՂԹԵԼՈՒԵՆՔ','#հաղթելուենք','#յաղթելուենք','#Ֆլեշմոբ']
        pt=[p.lower().strip() for p in pt]
        return set(pt)

    def cleanTwitterUinfoForBotDetection(self):
        #Prepares Twitter user data for bot detection
        uinfo = pickle.load(open('twitter_uinfo.p','rb'))

        w=['screen_name','In_reply','retweet_count','fav_count','total_usrmention','created_at_tweets','texts']

        w=['id','id_str','screen_name','created_at','location','description','url','followers_count','friends_count','listedcount','favourites_count','verified','statuses_count','lang','status','default_profile','default_profile_image','has_extended_profile','name','bot']
        statmap={}


        out=[]
        for k, uu in uinfo.items():
                u=uu
                dic={}
                if type(u) == dict:
                    try:
                        for a in w:
                            if a in ['bot','status']:
                                dic[a] = ''
                                continue
                            aa=a
                            if a == 'listedcount':
                                aa='listed_count'
                            dic[a] = u[aa]
                        try:
                            dic['status'] = statmap[u['id']]
                        except KeyError:
                            dic['status'] = ''
                        out.append(dic)
                    except KeyError:
                        continue

        with codecs.open('bot_userdata.csv','w', encoding='utf-8') as f:
            w=csv.DictWriter(f, delimiter=',', fieldnames=w)
            w.writeheader()
            w.writerows(out)

    def cleanTwitterDataForBotDetection(self):
        #Prepares Twitter data for bot detection
        uinfo = pickle.load(open('twitter_uinfo.p','rb'))
        self.readTwitter('../pres_25_5/data/tw', align=False, delete=False, skipFields=False)
        tweet_meta={}
        for k,v in self.tw.items():
            uid = v['author_id']
            sn=None
            try:
                try:
                    sn = uinfo[uid]['screen_name']
                except KeyError:
                    pass
            except TypeError:
                pass
            if not sn:
                continue
            tweet_meta[uid]={
                'screen_name': sn,
                'In_reply': 0,
                'retweet_count': 0,
                'fav_count': 0,
                'total_usrmention': 0,
                'created_at_tweets': []
            }
            try:
                if 'conversation_id' in v.keys() and v['conversation_id']:
                    tweet_meta[uid]['In_reply'] +=1
                elif 'reply_to' in v.keys() and v['reply_to']:
                    tweet_meta[uid]['In_reply'] +=1
                try:
                    tweet_meta[uid]['retweet_count'] += v['public_metrics']['retweet_count']
                except KeyError:
                    tweet_meta[uid]['retweet_count'] += int(v['retweet_count'])

                try:
                    tweet_meta[uid]['fav_count'] += v['public_metrics']['reply_count']
                except KeyError:
                    tweet_meta[uid]['fav_count'] += int(v['reply_count'])
                try:
                    mentions = len(re.findall(r'\b\@[a-z0-9_]+', v['text']))
                except KeyError:
                    mentions = len(re.findall(r'\b\@[a-z0-9_]+', v['body']))
                tweet_meta[uid]['total_usrmention'] += mentions
                try:
                    tweet_meta[uid]['created_at_tweets'].append(str(v['created_at']))
                except KeyError:
                    tweet_meta[uid]['created_at_tweets'].append(str(v['timestamp']))
            except KeyError:
                del tweet_meta[uid]
                continue
        out=[]
        for k,v in tweet_meta.items():
            out.append(v)
        del tweet_meta
        with codecs.open('all_cleaned_tweets.csv','w', encoding='utf-8') as f:
            w=csv.DictWriter(f, delimiter=',', fieldnames=['screen_name', 'In_reply', 'retweet_count', 'fav_count', 'total_usrmention', 'created_at_tweets'])
            w.writeheader()
            w.writerows(out)
    
    def reactions_conciliatory(self):
        #Outputs reactions to conciliatory posts
        facebook_emotes = pickle.load(open('facebook_emotes.p','rb'))
        out = [['date','fb_reaction','fb_reaction_value', 'score', 'keyword', 'source','text']]
        self.connect()

        mapper = self.readMapper()
        
        ct=0
        i=0
        self.cur.execute("select id, date, platform, text, message_id, score, newscore from posts")
        desc = [a[0] for a in self.cur.description]
        itmr = self.cur.fetchone()
        while itmr:
            itm = dict(zip(desc, itmr))
            itm['score'] = itm['score']
            if itm['newscore']:
                itm['score'] = itm['newscore']
            i+=1
            if i % 10000 == True:
                print(i/1000, 'RC')
            
            try:
                mapper[itm['id']]
            except KeyError:
                itmr=self.cur.fetchone()
                continue
            donethemes = []
            for theme, rows in self.all_themes.items():
                if not theme == 'conciliatory':
                    continue
                for row in rows:
                    term = self.makeWordOrIssue(row)

                    word = row['Word, hashtag or issue'].strip().lower()
                    keyword = "%s %s"% (word, row['AND'].strip().lower())
                    keyword =keyword.strip()
                    if term in mapper[itm['id']]:
                        added=False
                        reaction = None
                        data = [itm['date'], None,None, itm['score'], keyword, itm['text']]
                        if itm['platform'] == 'fb':
                            try:
                                try:
                                    ztm=facebook_emotes[itm['message_id']]
                                except KeyError:
                                    ztm=facebook_emotes[str(itm['message_id'])]
                            except KeyError:
                                ztm={}
                            if not ztm == {}:
                                print(ztm)
                                for xy in ['Love','Wow','Haha','Sad','Angry','Care']:
                                    if ztm[xy] > 0:
                                        reaction = xy
                                        reactionval = ztm[xy]
                                        data = [itm['date'], reaction, reactionval, itm['score'], keyword, itm['text']]
                                        out.append(data)
                                        print([reaction, reactionval])
                                        added=True
                        if not added:
                            out.append(data)
            itmr=self.cur.fetchone()
        self.writecsv('%s/reactions_conciliatory.csv' % self.outpath, out)
    
    def babyKillers(self):
        #find all posts that mention babies/children and killing in various languages
        babies =['baby','child','երեխա', 'uşaq', 'bala', 'малыш','ребенок']
        killers = ['убийство', 'öldürmək', 'սպանել','kill']
        out=[]
        for s in self.data:
            txt = s['text'].lower()
            for h in re.findall(self.hashtagregex, txt):
                for b in babies:
                    if b in h:
                        for k in killers:
                            if k in h:
                                # out.append([s['id'], h,txt, s['score'], s['date']])
                                out.append(h)
        from collections import Counter
        c=Counter(out)
        for k,v in c.items():
            if v > 10:
                print(k,v)

    def checkTheBots(self):
        #do bot detection analysis
        from botter import botter
        bot = botter()
        bot.loadData()
    
        bot.analyze()
        
    def fixbaduserids(self, user_id):
        #tries to fix bad user IDs.
        #Does not really work.
        oruserid=user_id
        if 'e' in user_id:
            user_id = user_id.split('e')[0].replace('.','')
        if user_id.endswith('.0'):
            user_id = user_id[:-2]
        try:
            user_id=int(user_id)
        except ValueError:
            user_id = oruserid
        return user_id

    def Botornot_hashtag(self):
        #Outputs bot scores (where available) per language
        if self.authscores == {}:
            self.findTheBots()
        out = [['id','date', 'botscore','partisanship','language']]
        tot=len(self.data)
        self.connect()
        mapper=self.readMapper()
        self.cur.execute("select * from posts")
        des = [a[0] for a in self.cur.description]
        itmr=self.cur.fetchone()
        i=0
        while itmr:
            i+=1 
            itm=dict(zip(des, itmr))
            try:
                score=self.authscores[self.fixbaduserids(str(itm['usr']))][0][0]
            except KeyError:
                itmr=self.cur.fetchone()
                continue
            qqq=randint(0,100)
            if not qqq == 5:
                itmr=self.cur.fetchone()
                continue
            if not itm['language'] in ['ru', 'en', 'az' , 'arm']:
                itm['language'] ='other'
            if i % 100000 == True:
                print('Botornot_hashtag', i, tot, len(out))
            qq = re.findall(self.hashtagregex, itm['text'])
            qq2=[]
            parts=[]
            for theme, rows in self.all_themes.items():
            
                for row in rows:
                    term = self.makeWordOrIssue(row)
                    try:
                        if term in mapper[itm['id']]:
                            parts.append(row['partisanship'])
                    except KeyError:
                        pass
            try:
                parts=list(set(parts))
                score=self.authscores[self.fixbaduserids(str(itm['usr']))][0][0]
                out.append([itm['id'], score, itm['date'] , ",".join(parts), itm['language']])
            except KeyError as err:
                pass
            itmr=self.cur.fetchone()
        print(len(out))
        self.writecsv('%s/Botornot_hashtag.csv' % self.outpath,out)

    def Botornot_authlevels(self):
        #Outputs authenticity levels for users
        if self.authscores == {}:
            self.findTheBots()
        out = [['language','auth_level', 'botscore']]
        tot=len(self.data)
        self.connect()
        self.cur.execute("select * from posts")
        des = [a[0] for a in self.cur.description]
        itmr=self.cur.fetchone()
        i=0
        while itmr:
            i+=1 
            itm=dict(zip(des, itmr))
            try:
                score=self.authscores[self.fixbaduserids(str(itm['usr']))][0][0]
            except KeyError:
                itmr=self.cur.fetchone()
                continue
            level=None
            if score <= 0.33:
                level='low'
            elif score <= 0.66:
                level='medium'
            else:
                level='high'
            if not itm['language'] in ['ru', 'en', 'az' , 'arm']:
                itm['language'] ='other'
            if i % 100000 == True: