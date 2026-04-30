from pymongo.errors import BulkWriteError
from pymongo import MongoClient
import time


class MongoLoader:
    def __init__(self, mongo_uri:str):
        self.mongo_uri = mongo_uri
        self.client_conn = self.get_mongodb_client()


    def get_mongodb_client(self):
        for retry in range(5):
            try:
                time.sleep(2)
                print("Try to connect to 'MongoDB'⏳...")
                client_conn = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=2000)
                client_conn.admin.command("ping")
                print("👍 Cnnected to 'MongoDB'!")
                return client_conn
            except Exception as e:
                print(f"👎 'MongoDB'-connection-attempt number {retry+1} failed: \n{e}")
                if retry == 4:
                    raise

    
    def insert(self, db:str, coll:str, logs:list):
        loader = self.client_conn[db][coll]
        try:
            result = loader.insert_many(documents=logs)
            print(f"Inserted {len(result.inserted_ids)} logs to 'MongoDB'")
        except BulkWriteError as e:
            print(f"Bulk write 'MongoDB' error:\n{e}")
            for err in e.details.get("writeErrors", []):
                print(f"Insert to 'MongoDB' Failed at index {err['index']}: {err['errmsg']}")


    def get_last_docs(self, db:str, coll:str, sum=10):
        collection = self.client_conn[db][coll]
        docs = collection.find().sort("_id", -1).limit(sum)
        return docs
    

    def get_docs_by_param(self, db:str, coll:str, defined_param:str):
        defined_param = defined_param.upper()
        collection = self.client_conn[db][coll]
        docs = collection.find({"log": {"$regex": defined_param}})
        return docs