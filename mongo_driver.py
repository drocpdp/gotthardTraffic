"""
Bootstrap/Initiate MongoDB.

Create/Enforce index validation

Uses pymongo library.

REMEMBER: MongoDB waits until you have inserted content before creating DB/collections

"""
from pymongo import MongoClient

class MongoDriver:

    def __init__(self):
        self.client = MongoClient()
        self.db = None
        self.collection = None

    def create_database(self, db_name):
        if db_name in self.client.list_database_names():
            print("DB EXISTS. NOT CREATED")
        else:
            mongodb = self.client[db_name]
            self.db = mongodb

    def create_collection(self, collection_name):
        if collection_name in self.db.list_collection_names():
            print("COLLECTION EXISTS. NOT CREATED")
        else:
            collection = self.db[collection_name]
            self.collection = collection


if __name__=="__main__":
    mi = MongoDriver()
    mi.create_database('testdb')
    mi.create_collection('testcollection')



