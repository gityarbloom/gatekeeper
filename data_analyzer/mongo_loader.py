from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError
from bson import ObjectId
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
                self.logger.publish_err_log(f"👎 'MongoDB'-connection-attempt number {retry+1} failed: {e}")
                if retry == 4:
                    raise


    
    def insert(self, doc_num, db:str, coll:str, doc:dict):
        loader = self.client_conn[db][coll]
        try:
            result = loader.update_one(
                {"suspect_id": doc["suspect_id"]},
                {"$setOnInsert": doc}
                ,upsert=True
                )
            self.logger.publish_info_log(f"Insert doc \n**Number {doc_num}** \nto MongoDB\nInserted id: {result.upserted_id}")
        except PyMongoError as e:
            self.logger.publish_err_log(f"'MongoDB' error: \n{e}")