import praw
import re
import sqlite3
from urllib.parse import urlparse


reddit = praw.Reddit('bot1')
subreddit = reddit.subreddit('all')

db = sqlite3.connect('data.sqlite')
cursor = db.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS urls (id TEXT PRIMARY KEY, host TEXT, url TEXT, subreddit TEXT, score INTEGER, gilds INTEGER, time INTEGER, author TEXT)')
print('found database')
count = 0

try:
    for comment in subreddit.stream.comments():
        text = comment.body
        score = comment.score

        #find urls in text if any exist
        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
        for url in urls:
            #print(url)

            #There can be trailing ) ] > due to reddit formatting, cut them here
            if(url[len(url) - 1] == ')' or url[len(url) - 1] == ']' or url[len(url) - 1] == '>'):
                url = url[:-1]

            #parse url to get host
            parsed_url = urlparse(url)

            #remove subdomain because urlparse gives with it
            #hostname = parsed_url.hostname.split('.', 1)[1]
            hostname = parsed_url.hostname
            if(hostname.startswith('www.')):
                hostname = hostname[4:]

            #generate unique id to prevent same links from same comment being added multiple times
            id = comment.id + "-" + url
            #store entry or ignore if one already exists
            cursor.execute('INSERT OR IGNORE INTO urls (id, host, url, subreddit, score, gilds, time, author) VALUES (?,?,?,?,?,?,?,?)',
                            (id, hostname, url, comment.subreddit.display_name, comment.score, comment.gilded, round(comment.created_utc), comment.author.name))
        db.commit()

#end program gracefully if closed with console ctrl+c
except KeyboardInterrupt:
    db.commit()
    db.close()

#TODO comments
#TODO logging whats happenign and how much data processed etc
#TODO companion script to read data
