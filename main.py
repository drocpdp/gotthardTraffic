import os
from pymongo import MongoClient

# next step, scrub for ALL .log files

#location = '/Users/davideynon/Projects/logs/pytwitservice/2022_10_29-14_24_37_log.log'
location = '/Users/davideynon/Projects/logs/pytwitservice/'
data = []
idx = 0

client = MongoClient()

db = client.gottardo
collection = db.tweets

total = 0
ids = []

from_directions = set()
to_directions = set()

import os
for file in os.listdir(location):
    if file.endswith("log.log"):
        log_file = os.path.join(location,file)
        print(log_file)
        with open(log_file, 'r', encoding='utf-8') as log_data:
            for line in log_data:
                tweet = {}
                split_line = line.split("UserName=")
                if len(split_line) > 1 and split_line[1].startswith('TCSGottardo'):
                    tweet['UserName'] = 'TCSGottardo'
                    tmp = split_line[1].split('Created=')[1]
                    tweet['date'] = tmp.split(',')[0]

                    tweet_txt = tmp.split(',',1)[1].split('Text=')[1].split('-')
                    tweet['route'] = tweet_txt[0].strip()
                    tweet['from_direction'] = tweet_txt[1].strip()
                    from_directions.add(tweet['from_direction'])
                    tweet['to_direction'] = tweet_txt[2].strip()
                    to_directions.add(tweet['to_direction'])
                    tweet['info'] = ",".join(tweet_txt[3:]).strip()
                    post_id = collection.insert_one(tweet)
                    print(post_id.acknowledged, post_id.inserted_id)
                    ids.append(post_id)
        #print(len(ids))

        for post in collection.find({"to_direction":"chiasso"}):
            print(post)

        total += collection.count_documents({})

print(total)
print(len(ids))

print(from_directions)
print(to_directions)