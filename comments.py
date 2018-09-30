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
    cursor.execute('CREATE TABLE IF NOT EXISTS comments (id TEXT PRIMARY KEY, comment_id TEXT, host TEXT, url TEXT, subreddit TEXT, score INTEGER, gilds INTEGER, time INTEGER, author TEXT, nsfw INT)')
    print('---Init Complete---\n\n')

    comment_count = 0.0
    total_time = 0.0
    total_count = 0
    try:
        while True:
            print('----------Begin one loop----------')
            start_time = time.time()
            handle_comments(db, cursor, subreddit)
            print('Comments execution time: ' + str(time.time()-start_time))
            comment_count += 1.0
            total_time += time.time()-start_time
            print('Comments average execution time: ' + str(total_time/comment_count))
            print('----------Completed one loop----------\n\n')
    #end program gracefully if closed with console ctrl+c
    except KeyboardInterrupt:
        db.commit()
        db.close()




def handle_comments(db, cursor, subreddit):
    print('---Getting Comments---')
    count = 0
    comments = subreddit.comments(limit=100)
    for comment in comments:
        text = comment.body

        cursor.execute('SELECT * FROM comments WHERE comment_id = ?', (comment.id,))
        rows = cursor.fetchall()
        if(len(rows) > 0):
            continue

        #find urls in text if any exist
        urls = re.findall('(http[s]?://[^\s]+)', text)
        for url in urls:
            #There can be trailing ) ] > due to reddit formatting, cut them here
            if(url[len(url) - 1] == ')' or url[len(url) - 1] == ']' or url[len(url) - 1] == '>'):
                url = url[:-1]

            #parse url to get host
            try:
                parsed_url = urlparse(url)
                #remove subdomain because urlparse gives with it
                #hostname = parsed_url.hostname.split('.', 1)[1]
                hostname = parsed_url.hostname
                if(hostname.startswith('www.')):
                    hostname = hostname[4:]

                #generate unique id to prevent same links from same comment being added multiple times
                id = comment.id + "-" + url
                #store entry or ignore if one already exists
                cursor.execute('INSERT OR IGNORE INTO comments (id, comment_id, host, url, subreddit, score, gilds, time, author, nsfw) VALUES (?,?,?,?,?,?,?,?,?,?)',
                                (id, comment.id, hostname, url, comment.subreddit.display_name, comment.score, comment.gilded, round(comment.created_utc),
                                comment.author.name, comment.submission.over_18))
            except ValueError:
                print('ERROR: Invalid url - ' + url)


        db.commit()
        count += 1


    print('Processed ' + str(count) + ' comments')




#cProfile.run('main()', 'profile.prof_file')
main()

#TODO comments
#TODO logging whats happenign and how much data processed etc
#TODO companion script to read data
#TODO print tri-hourly report
#TODO check for golds after a week
