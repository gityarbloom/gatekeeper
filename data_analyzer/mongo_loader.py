# from pymongo import MongoClient
# import os


# class InitMongoClientDB:
#     def __init__(self, mongo_uri, db_name):
#         self.mongo_uri = 
#         self.db_name = db_name
#         self.client = self.get_mongodb_client()


#     def get_mongodb_client(self):
#         for retry in range(5):
#             try:
#                 time.sleep(2)
#                 print("\nTry to connect to MySql⏳...")
#                 init_client = MongoClient(self.mongo_uri)
#                 print(f"\n👍 Cnnected to DB {db_name} in MongoDB!")
#                 return conn
#             except Exception as e:
#                 print(f"\n👎 Attempt {retry+1} failed: {e}")
#                 if retry == 4:
#                     raise

# db = client["mydatabase"]

# collection = db["mycollection"]

# doc = {"name": "Alice", "age": 30}
# collection.insert_one(doc)

# for item in collection.find():
#     print(item)