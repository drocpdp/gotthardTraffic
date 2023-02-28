import os, sys
import json
from pymongo import MongoClient
import urllib

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
import os
from mongo_driver import MongoDriver

class ETL(MongoDriver):

    def __init__(self):
        # all .log files
        self.location = os.environ['GOTTHARD_TRAFFIC_LOGS']

        # all acceptable Twitter Users/handles
        self.expected_users = ['TCSGottardo']
        
        self.data = []
        self.idx = 0
    
    def etl(self):
        client = self.get_connection_uri_connection_string()

        db = client.gottardo

        collection = db.tweets

        total = 0

        ids = []

        from_directions = set()
        to_directions = set()

        file_object = None

        for dir_obj in os.scandir(self.location): # generator

            log_file = dir_obj.path

            # --- log.log files
            if log_file.endswith("log.log"): 

                for line in (ln for ln in open(log_file, 'r', encoding='utf-8')): # generator
                    
                    try:
                        tweet = {}
                        #print(line)
                        
                        # transform to our accepted mongodb entry criteria
                        # Tweet ID (key=id)
                        tweet['id'] = line.split('ID=')[1].split(',')[0]

                        # User (key=user)
                        tweet['user'] = line.split('UserName=')[1].split(',')[0]

                        # Created (key=created)
                        tweet['created'] = line.split('Created=')[1].split(',')[0]

                        # Content (key=content)
                        tweet['content'] = line.split('Text=')[1]

                        #print(tweet)

                        if tweet['user'] in self.expected_users:
                            # insert into mongodb
                            post_id = collection.insert_one(tweet)
                            print(post_id.acknowledged, post_id.inserted_id)

                    except Exception as e:
                        #print("NOT ADDED-->", line)
                        continue               
            
            # --- json files
            elif log_file.endswith(".json"):
                with open(log_file, 'r', encoding='utf-8') as json_log:
                    data = json.load(json_log)
                    for tweet in data:
                        try:
                            post_id = collection.insert_one(tweet)
                            print(post_id.acknowledged, post_id.inserted_id)
                        except Exception as e:
                            continue

        print(collection.count_documents({}), 'total unique')        


if __name__=="__main__":
    ETL().etl()
