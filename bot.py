import praw
import re
import sqlite3
from urllib.parse import urlparse


reddit = praw.Reddit('bot1')
subreddit = reddit.subreddit('PagingExpertsBot')

db = sqlite3.connect('data.sqlite')
cursor = db.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS urls (host TEXT, url TEXT, subreddit TEXT, score INTEGER, gilds INTEGER, time INTEGER, author TEXT)')
print('found database')
count = 0

try:
    for comment in subreddit.stream.comments():
        #cursor.execute('SELECT * FROM urls WHERE id = ?', [comment.id])
        #rows = cursor.fetchall()
        #if (len(rows) > 1):
        #    print('ERROR\n NON UNIQUE id ' + comment.id + ' IN TABLE urls')
        #    exit()


        #elif (rows != None):
            #print('processing comment')
        text = comment.body
        score = comment.score

        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\), ]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
        for url in urls:
            parsed_url = urlparse(url)
            #remove subdomain
            hostname = parsed_url.hostname.split('.', 1)[1]

            cursor.execute('SELECT * FROM urls WHERE host = ? AND url = ? AND subreddit = ? AND score = ? AND gilds = ? AND time = ? AND author = ?', (hostname, url, comment.subreddit.display_name, comment.score, comment.gilded, round(comment.created_utc), comment.author.name))
            rows = cursor.fetchall()
            if (len(rows) > 1):
                print('ERROR\n DUPLICATE ROW ' + comment.id + ' IN TABLE urls')
                exit()

            elif(len(rows) == 0):
                cursor.execute('INSERT INTO urls (host, url, subreddit, score, gilds, time, author) VALUES (?,?,?,?,?,?,?)',(hostname, url, comment.subreddit.display_name, comment.score, comment.gilded, round(comment.created_utc), comment.author.name))


except KeyboardInterrupt:
    db.commit()
    db.close()

#TODO comments
#TODO logging whats happenign and how much data processed etc
#TODO companion script to read data
