import praw
import re
import sqlite3
import time
#import cProfile
from urllib.parse import urlparse

def main():
    reddit = praw.Reddit('bot1')
    subreddit = reddit.subreddit('all')

    db = sqlite3.connect('data.sqlite')
    cursor = db.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, post_id TEXT, host TEXT, url TEXT, subreddit TEXT, title TEXT, time TEXT, author TEXT, nsfw INT)')
    print('---Init Complete---\n\n')

    try:
        while True:
            print('----------Begin one loop----------')
            start_time = time.time()
            handle_submissions(db, cursor, subreddit)
            print('Submissions execution time: ' + str(time.time()-start_time))
            print('----------Completed one loop----------\n\n')
            time.sleep(0.1)
    #end program gracefully if closed with console ctrl+c
    except KeyboardInterrupt:
        db.commit()
        db.close()




def handle_submissions(db, cursor, subreddit):
    print('---Getting Submissions---')
    submissions = subreddit.new(limit=100)
    count = 0
    for submission in submissions:

        cursor.execute('SELECT * FROM posts WHERE post_id = ?', (submission.id,))
        rows = cursor.fetchall()
        if(len(rows) > 0):
            continue


        if(submission.is_self):
            text = submission.selftext
            urls = re.findall('(http[s]?://[^\s]+)', text)
            for url in urls:
                try:
                    parsed_url = urlparse(url)

                    hostname = parsed_url.hostname
                    if(hostname.startswith('www.')):
                        hostname = hostname[4:]

                    id = submission.id + "-" + url
                    cursor.execute('INSERT OR IGNORE INTO posts (id, post_id, host, url, subreddit, title, time, author, nsfw) VALUES (?,?,?,?,?,?,?,?,?)',
                                    (id, submission.id, hostname, url, submission.subreddit.display_name, submission.title, round(submission.created_utc),
                                    submission.author.name, submission.over_18))
                except ValueError:
                    print('ERROR: Invalid url - ' + url)


        else:
            url = submission.url
            parsed_url = urlparse(url)

            hostname = parsed_url.hostname
            if(hostname.startswith('www.')):
                hostname = hostname[4:]

            id = submission.id + "-" + url
            cursor.execute('INSERT OR IGNORE INTO posts (id, post_id, host, url, subreddit, title, time, author, nsfw) VALUES (?,?,?,?,?,?,?,?,?)',
                            (id, submission.id, hostname, url, submission.subreddit.display_name, submission.title, round(submission.created_utc),
                            submission.author.name, submission.over_18))
        db.commit()
        count += 1
    print('Processed ' + str(count) + ' comments')

main()
#print('Processed ' + str(count) + ' submissions')
