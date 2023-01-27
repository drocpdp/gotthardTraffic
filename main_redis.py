"""
Beginnings. First, parse from log, experiment with redis storage.
This will later go into pull-log feature, and refactored to also include
functionality to enter into redis directly from pytwitterfeed extraction.
"""
import redis
import json

location = '/Users/davideynon/Projects/logs/pytwitservice/2022_10_29-14_24_37_log.log'

data = []

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
redis = redis.Redis(connection_pool=pool)

idx = 0

with open(location, 'r', encoding='utf-8') as log_data:
    for line in log_data:
        tweet = {}
        split_line = line.split("UserName=")
        if len(split_line) > 1 and split_line[1].startswith('TCSGottardo'):
            print(line)
            tweet['UserName'] = 'TCSGottardo'
            tmp = split_line[1].split('Created=')[1]
            tweet['date'] = tmp.split(',')[0]

            tweet_txt = tmp.split(',',1)[1].split('Text=')[1].split('-')
            tweet['route'] = tweet_txt[0].strip()
            tweet['from_direction'] = tweet_txt[1].strip()
            tweet['to_direction'] = tweet_txt[2].strip()
            tweet['info'] = ",".join(tweet_txt[3:]).strip()
            redis.hset(name='tweets.{}'.format(idx),key=None, value=None, mapping=tweet)
            idx += 1
    print(redis.hgetall('tweets'))