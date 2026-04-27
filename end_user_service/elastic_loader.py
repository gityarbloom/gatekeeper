from moduls import ElasticSearchClient
from moduls import MongoLoader
import time
import os



class ElasticLoader:
    _program_running = None
    _initialized = False

    def __new__(cls):
        if cls._program_running is None:
            cls._program_running = super().__new__(cls)
        return cls._program_running


    def __init__(self):
        if self.__class__._initialized:
            return
        self.mongo = MongoLoader(os.getenv("MONGO_URI", "mongodb://admin:israelyarbloom@localhost:27017/?authSource=admin"))
        self.es = ElasticSearchClient("http://localhost:9200", "gate_keeper", mapping=self.get_map())
        self.play_program()


    @staticmethod
    def get_map():
        return {
            "mappings": {
                "properties": {
                "message_id": { "type": "keyword" },
                "suspect_id": { "type": "keyword" },
                "text": { "type": "text" },
                "tags": { "type": "keyword" }
                }
            }
        }


    @staticmethod
    def transform(doc:dict):
        return {
            "message_id": doc["message_id"],
            "suspect_id": doc["suspect_id"],
            "text": doc["text"],
            "tags": doc["tags"]
        }

    
    def play_program(self):
        self.__class__._initialized = True
        print("\n\n\n🌞 End-user service START!! 🌞\n\n\n")
        time.sleep(5)
        
        counter = 0
        for doc in self.mongo.get_all_docs("gatekeeper_db", "gatekeeper_coll"):
            counter += 1
            print(f"\n\n****Received from MongoDB a new DOC --number {counter}--****")
            print(f"DOC: \n{doc}\n")
            es_doc = self.transform(doc)
            response = self.es.index_doc(es_doc["message_id"], es_doc)
            print("\n🆕 ES index 'gate_keeper' is receive a new document.")
            print(f"Document-status in ES: \n'{response["result"]}'\n"+"*"*75+"\n")
            time.sleep(1)

        time.sleep(5)
        print("\n\n\n🥱 End-user service FINSHED!! 🥱\n\n\n")