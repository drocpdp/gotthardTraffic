import os, sys
from pymongo import MongoClient

"""
Extract all log files, add to db

Transform to our accepted mongodb entry criteria
    Tweet ID (key=id)
    User (key=user)
    Created (key=created)
    Content (key=content)

Load to MongoDB

example:
  {
    "id": 1611713256246185984,
    "user": "TCSGottardo",
    "created": "2023-01-07 13:15:59+00:00",
    "content": "#a2 - chiasso -&gt; s. gottardo - tra qu..."
  },...

os.scandir - returns generator object


"""


# all .log files
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

file_object = None

for dir_obj in os.scandir(location): # generator

    log_file = dir_obj.path

    if log_file.endswith("log.log"):

        for line in (ln for ln in open(log_file, 'r', encoding='utf-8')): # generator
            
            try:
                tweet = {}
                print(line)
                
                # transform to our accepted mongodb entry criteria
                # Tweet ID (key=id)
                tweet['id'] = line.split('ID=')[1].split(',')[0]

                # User (key=user)
                tweet['user'] = line.split('UserName=')[1].split(',')[0]

                # Created (key=created)
                tweet['created'] = line.split('Created=')[1].split(',')[0]

                # Content (key=content)
                tweet['content'] = line.split('Text=')[1]

                print(tweet)
            except Exception as e:
                print("NOT ADDED-->", line)
                continue


        # insert into mongodb
        post_id = collection.insert_one(tweet)
        print(post_id.acknowledged, post_id.inserted_id)
        
        ids.append(post_id)
        print(len(ids))

        for post in collection.find({}):
            print(post)

        total += collection.count_documents({})

print(total)
print(len(ids))

print(from_directions)
print(to_directions)