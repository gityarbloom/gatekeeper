from end_user_modules import ElasticSearchClient, MongoLoader
import time
import os




class ServiceManager:
    time.sleep(60)
    _program_running = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._program_running is None:
            cls._program_running = super().__new__(cls)
        return cls._program_running


    def __init__(self, logger):
        if self.__class__._initialized:
            return
        self.logger = logger
        self.mongo = MongoLoader(logger, os.getenv("MONGO_URI", "mongodb://admin:israelyarbloom@localhost:27017/?authSource=admin"))
        self.es = ElasticSearchClient(logger, os.getenv("ELS_URI", "http://localhost:9200"), "gate_keeper", mapping=self.get_map())
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
        self.logger.publish_info_log("****🌞 End-user service START!! 🌞****")
        time.sleep(5)
        
        counter = 0
        for doc in self.mongo.get_all_docs("gatekeeper_db", "gatekeeper_coll"):
            counter += 1
            self.logger.publish_info_log(f"Received from 'MongoDB' a new DOC. \n**Number {counter}**\nRecived doc: \n{doc}")
            es_doc = self.transform(doc)
            self.es.index_doc(es_doc["message_id"], es_doc)
            self.logger.publish_info_log(f"🆕 Indexed a doc to 'gate_keeper' ES-index \n**Number {counter}")
            time.sleep(1)

        time.sleep(5)
        self.logger.publish_info_log("****🥱 'End-user' service FINSHED!! 🥱****")