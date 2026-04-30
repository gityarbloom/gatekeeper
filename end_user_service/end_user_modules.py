from pymongo.errors import BulkWriteError, PyMongoError
from elasticsearch import Elasticsearch
from pymongo import MongoClient
import time



class MongoLoader:
    def __init__(self, logger, mongo_uri:str):
        self.logger = logger
        self.mongo_uri = mongo_uri
        self.client_conn = self.get_mongodb_client()


    def get_mongodb_client(self):
        for retry in range(5):
            try:
                time.sleep(2)
                self.logger.publish_info_log("Try to connect to 'MongoDB'⏳...")
                client_conn = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=2000)
                client_conn.admin.command("ping")
                self.logger.publish_info_log("👍 Cnnected to 'MongoDB'!")
                return client_conn
            except Exception as e:
                self.logger.publish_err_log(f"👎 'MongoDB'-connection-attempt number {retry+1} failed: \n{e}")
                if retry == 4:
                    raise

    
    def insert(self, doc_num, db:str, coll:str, docs:list[dict]):
        loader = self.client_conn[db][coll]
        if len(docs) > 1:
            self.logger.publish_info_log("Insert A batch of documents to MongoDB")
            try:
                result = loader.insert_many(documents=docs)
                self.logger.publish_info_log(f"Inserted {len(result.inserted_ids)} docs to 'MongoDB'")
            except BulkWriteError as e:
                self.logger.publish_err_log(f"Bulk write 'MongoDB' error:\n{e}")
                for err in e.details.get("writeErrors", []):
                    self.logger.publish_err_log(f"Insert to 'MongoDB' Failed at index {err['index']}: {err['errmsg']}")
        try:
            result = loader.insert_one(docs[0])
            self.logger.publish_info_log(f"Insert doc number {doc_num} to 'MongoDB'\nInserted id: {result.inserted_id}")
        except PyMongoError as e:
            self.logger.publish_err_log(f"'MongoDB' error: \n{e}")
            
            
    def get_all_docs(self, db:str, coll:str):
        for doc in self.client_conn[db][coll].find():
            yield doc



#########################################



class ElasticSearchClient:
    def __init__(self, logger, host:str, index_name:str, mapping:dict):
        self.logger = logger
        self.es_host = host
        self.index_name = index_name
        self.mapping = mapping
        self.es = self.start_es_conn()


    def start_es_conn(self):
        for retry in range(5):
            try:
                time.sleep(2)
                self.logger.publish_info_log("Try to connect to ElasticSearch⏳...")
                es = Elasticsearch(self.es_host)
                response = es.indices.create(index=self.index_name, body=self.mapping)
                self.logger.publish_info_log(f"👍 ElasticSearch is cnnected!")
                self.logger.publish_info_log(f"👍 Index {self.index_name} Created!\n{response}")
                return es
            except Exception as e:
                self.logger.publish_err_log(f"👎 Attempt {retry+1} failed: {e}")
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