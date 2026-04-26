import os



class AnalyzerConfig:
    def __init__(self):
        self.mysql = {
            "password": os.getenv("MYSQL_PASSWORD"),
            "host": os.getenv("MYSQL_HOST"),
            "db_name": os.getenv("MYSQL_DATABASE"),
            "user": os.getenv("MYSQL_USER")
        }
        self.kafka = {
            'bootstrap.servers': os.getenv("CONSUM_CONFIG"),
            'group.id': 'gatekeeper',
            'auto.offset.reset': 'earliest'
            }
        self.mongo = os.getenv("MONGO_URI", "mongodb://localhost:27017/")