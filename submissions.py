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
    cursor.execute('CREATE TABLE IF NOT EXISTS posts (id TEXT PRIMARY KEY, post_id TEXT, host TEXT, url TEXT, subreddit_id TEXT, title TEXT, time TEXT, author TEXT, nsfw INT, self INT)')
    print('---Init Complete---\n\n')

    start_time = time.time()
    count = 0
    try:
        for submission in subreddit.stream.submissions():
            handle_submission(db, cursor, submission)
            count += 1

            if(count == 100):
                count = 0
                print('----------Completed 100 Submissions----------')
                print('Average Execution time: ' + str((time.time()-start_time)/100))
                print('\n\n')
                start_time = time.time()
    #end program gracefully if closed with console ctrl+c
    except KeyboardInterrupt:
        db.commit()
        db.close()




def handle_submission(db, cursor, submission):

    #Check if submission already was process and can be ignored
    cursor.execute('SELECT * FROM posts WHERE post_id = ?', (submission.id,))
    rows = cursor.fetchall()
    if(len(rows) > 0):
        return

    #Check for urls in post if a selfpost
    if(submission.is_self):
        text = submission.selftext
        urls = re.findall('(http[s]?://[^\s]+)', text)
        for url in urls:
            try:
                url.rstrip('*]>)')
                parsed_url = urlparse(url)

                hostname = parsed_url.hostname
                if(hostname.startswith('www.')):
                    hostname = hostname[4:]

                id = submission.id + "-" + url
                cursor.execute('INSERT OR IGNORE INTO posts (id, post_id, host, url, subreddit_id, title, time, nsfw, self) VALUES (?,?,?,?,?,?,?,?,?)',
                                (id, submission.id, hostname, url, submission.subreddit_id, submission.title, round(submission.created_utc),
                                submission.over_18, submission.is_self))
            except ValueError:
                print('ERROR: Invalid url - ' + url)


    else:
        url = submission.url
        parsed_url = urlparse(url)

        hostname = parsed_url.hostname
        if(hostname.startswith('www.')):
            hostname = hostname[4:]

        id = submission.id + "-" + url
        cursor.execute('INSERT OR IGNORE INTO posts (id, post_id, host, url, subreddit_id, title, time, nsfw, self) VALUES (?,?,?,?,?,?,?,?,?)',
                        (id, submission.id, hostname, url, submission.subreddit_id, submission.title, round(submission.created_utc),
                        submission.over_18, submission.is_self))
    db.commit()

main()
#print('Processed ' + str(count) + ' submissions')
