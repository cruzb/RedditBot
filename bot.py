import sqlite3
import requests
import ujson as json
import re
import time
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
from pprint import pprint

PUSHSHIFT_URL = "https://apiv2.pushshift.io/reddit"
SUBREDDIT = "news"
TYPE = "comment"
ALL_STOP_WORDS = set(stopwords.words('english')) | set(string.punctuation)

def main():
    db = sqlite3.connect('data.sqlite')
    global cursor
    cursor = db.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS news_comments (word TEXT PRIMARY KEY, score INTEGER, silvers INTEGER, golds INTEGER, platinums INTEGER, gilds INTEGER, total INTEGER)')
    print('found database')
    #cursor.execute('CREATE UNIQUE INDEX idx_word ON news_comments(word)')

    try:
        process()
    except KeyboardInterrupt:
        db.commit()
        db.close()
    db.commit()
    db.close()


def fetchObjects():
    # Default params values
    r = requests.get(PUSHSHIFT_URL + "/" + TYPE + "/search/?subreddit=" + SUBREDDIT + "&sort_type=created_utc&size=1000")
    if r.status_code == 200:
        response = json.loads(r.text)
        data = response['data']
        sorted_data = sorted(data, reverse=True, key=lambda x: x['created_utc'])
        return sorted_data

def process():
    max_created_utc = 0
    max_id = 0
    #file = open("data.json","w")
    while 1:
        nothing_processed = True
        objects = fetchObjects()
        for object in objects:
            id = int(object['id'],36)
            if id > max_id:
                nothing_processed = False
                created_utc = object['created_utc']
                max_id = id
                if created_utc > max_created_utc:
                    max_created_utc = created_utc
                    process_comment(object)
                    # Code to do something with comment goes here ...
        if nothing_processed: return
        max_created_utc -= 1
        time.sleep(.5)


def process_comment(comment):
    pprint(comment)
    score = comment['score']
    text = comment['body'].lower()
    silvers = comment['gildings']['gid_1']
    golds = comment['gildings']['gid_2']
    platinums = comment['gildings']['gid_3']
    gilds = silvers + golds + platinums
    if(text == '[removed]'):
        return
    print(text)
    tokens = tokenize_string(text)
    print('\n\n')
    print(tokens)
    '''
    for token in tokens:
        cursor.execute('SELECT * FROM news_comments WHERE word = ?', token)
        entry = cursor.fetchone()
        if entry is None: #if fetch is empty then need to add entry
            cursor.execute('INSERT INTO news_comments (word, score, silvers, golds, platinums, gilds, total) VALUES (?,?,?,?,?,?,?)', (token, score, silvers, golds, platinums, gilds, 1))
        else: #entry exists, modify it
            pprint(entry)
            cursor.execute('REPLACE INTO news_comments(word, score, silvers, golds, platinums, gilds, total) VALUES(?,?,?,?,?,?,?)', (token, score + entry[1], silvers + entry[2], golds + entry[3], platinums + entry[4], gilds + entry[5], entry[6] + 1))
    '''

def tokenize_string(text):
    word_tokenized_text = word_tokenize(text)
    tokens = []
    for word in word_tokenized_text:
        if word not in ALL_STOP_WORDS:
            tokens.append(word)
    return tokens

main()
#get comments from posts in news
#remove ones that are not gilded or have less than threshold upvotes
#parse string to remove irrelevent stuff or do nltk processing
#store
