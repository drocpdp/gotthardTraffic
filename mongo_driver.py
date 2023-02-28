"""
Bootstrap/Initiate MongoDB.

Create/Enforce index validation

Uses pymongo library.

REMEMBER: MongoDB waits until you have inserted content before creating DB/collections

"""
import os
import urllib
from pymongo import MongoClient

class MongoDriver:

    def __init__(self):
        self.client = self.get_connection_uri_connection_string()
        self.db = None
        self.collection = None
    
    def _get_username(self):
        return os.environ['MONGODB_ATLAS_USERNAME']
    
    def _get_encoded_password(self):
        plaintext_pw = os.environ['MONGODB_ATLAS_PASSWORD']
        encoded_pw = urllib.parse.quote(plaintext_pw)
        return encoded_pw
    
    def _get_cluster_uri(self):
        return os.environ['MONGODB_ATLAS_CLUSTER_URI']
    
    def _get_connection_string(self):
        user = self._get_username()
        password = self._get_encoded_password()
        cluster_uri = self._get_cluster_uri()
        connect = f"mongodb+srv://{user}:{password}@{cluster_uri}/?retryWrites=true&w=majority"
        return connect
    
    def get_connection_uri_connection_string(self):
        client = MongoClient(self._get_connection_string(),tlsAllowInvalidCertificates=True)
        return client

    def get_database(self, db_name):
        db = self.client.get_database(db_name)
        self.db = db
        return self.client.get_database(db_name)

    def get_collection(self, collection_name):
        coll = self.db[collection_name]
        self.collection = coll
        return coll


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

    def create_index(self, collection_name, fields=None):
        self.collection.create_index([("id",1)], unique=True)

    def delete_all(self):
        self.collection.delete_all({})
        self.collection.delete_collection()


if __name__=="__main__":
    mi = MongoDriver()
    #mi.get_database('gottardo')
    #mi.get_collection('tweets')
    
    mi.create_database('gottardo')
    mi.create_collection('tweets')
    mi.create_index('')



