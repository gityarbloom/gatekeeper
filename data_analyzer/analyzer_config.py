import os



class AnalyzerConfig:
    def __init__(self):
        self.mysql = {
            "password": os.getenv("MYSQL_PASSWORD"),
            "host": os.getenv("MYSQL_HOST"),
            "db_name": os.getenv("MYSQL_DATABASE"),
            "user": os.getenv("MYSQL_USER")
        }
        self.consum = {
            'bootstrap.servers': os.getenv("KAFKA_URI"),
            'group.id': 'gatekeeper',
            'auto.offset.reset': 'earliest'
            }
        self.prod = {'bootstrap.servers': os.getenv("KAFKA_URI")}
        self.mongo = os.getenv("MONGO_URI", "mongodb://localhost:27017/")