from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError
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
                self.logger.publish_log("Try to connect to 'MongoDB'⏳...")
                client_conn = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=2000)
                client_conn.admin.command("ping")
                self.logger.publish_log("👍 Cnnected to 'MongoDB'!")
                return client_conn
            except Exception as e:
                self.logger.publish_log(f"👎 'MongoDB'-connection-attempt number {retry+1} failed: {e}")
                if retry == 4:
                    raise


    
    def insert(self, db:str, coll:str, docs:list[dict]):
        loader = self.client_conn[db][coll]
        if len(docs) > 1:
            self.logger.publish_log("Insert A batch of documents to MongoDB")
            try:
                result = loader.insert_many(documents=docs)
                self.logger.publish_log(f"Inserted {len(result.inserted_ids)} docs to 'MongoDB'\n")
            except BulkWriteError as e:
                self.logger.publish_log("Bulk write 'MongoDB' error:\n")
                for err in e.details.get("writeErrors", []):
                    self.logger.publish_log(f"Insert to 'MongoDB' Failed at index {err['index']}: {err['errmsg']}\n")

        self.logger.publish_log("Insert one doc to MongoDB")
        try:
            result = loader.insert_one(docs[0])
            self.logger.publish_log("Inserted id:", result.inserted_id)
        except PyMongoError as e:
            self.logger.publish_log(f"'MongoDB' error: \n{e}")