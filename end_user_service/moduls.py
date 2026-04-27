from pymongo.errors import BulkWriteError, PyMongoError
from elasticsearch import Elasticsearch
from pymongo import MongoClient
import time



class MongoLoader:
    def __init__(self, mongo_uri:str):
        self.client_conn = self.get_mongodb_client(mongo_uri)


    def get_mongodb_client(self, mongo_uri):
        for retry in range(5):
            try:
                time.sleep(2)
                print("\nTry to connect to MongoDB⏳...")
                client_conn = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
                client_conn.admin.command("ping")
                print(f"\n👍 Cnnected!")
                return client_conn
            except Exception as e:
                print(f"\n👎 Attempt {retry+1} failed: {e}")
                if retry == 4:
                    raise

    
    def insert(self, db:str, coll:str, docs:list[dict]):
        loader = self.client_conn[db][coll]
        if len(docs) > 1:
            print(f"\n\nInsert A batch of documents to MongoDB")
            try:
                result = loader.insert_many(documents=docs)
                print(f"\nInserted {len(result.inserted_ids)} docs\n")
            except BulkWriteError as e:
                print("\nBulk write error:\n")
                for err in e.details.get("\nwriteErrors", []):
                    print(f"\nFailed at index {err['index']}: {err['errmsg']}\n")

        print(f"\n\nInsert one doc to MongoDB")
        try:
            result = loader.insert_one(docs[0])
            print("\nInserted id:", result.inserted_id)
        except PyMongoError as e:
            print("\nMongo error:", e)


    def get_all_docs(self, db:str, coll:str):
        for doc in self.client_conn[db][coll].find():
            yield doc



#########################################



class ElasticSearchClient:
    def __init__(self, host:str, index_name:str, mapping:dict):
        self.es_host = host
        self.index_name = index_name
        self.mapping = mapping
        self.es = self.start_es_conn()


    def start_es_conn(self):
        for retry in range(5):
            try:
                time.sleep(2)
                print("\nTry to connect to ElasticSearch⏳...")
                es = Elasticsearch(self.es_host)
                response = es.indices.create(index=self.index_name, body=self.mapping)
                print(f"\n👍 ElasticSearch is cnnected!")
                print(f"\n\n👍 Index {self.index_name} Created!\n{response}\n")
                return es
            except Exception as e:
                print(f"\n👎 Attempt {retry+1} failed: {e}\n")
                if retry == 4:
                    raise


    def index_doc(self, doc_id:str, document:dict):
        return self.es.index(
            index=self.index_name,
            id=doc_id,
            document=document
        )
    

    def get_doc(self, doc_id:str):
        return self.es.get(index=self.index_name, id=doc_id)


    def search(self, query:str, size:int =10):
        body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "message_id",
                        "suspect_id",
                        "text",
                        "tags^5"
                    ]
                }
            },
            "size": size
        }
        return self.es.search(index=self.index_name, body=body)


    def delete_doc(self, doc_id:str):
        return self.es.delete(index=self.index_name, id=doc_id)