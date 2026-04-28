import os


class LoaderConfig:
    def __init__(self):
        self.prod_config = {"bootstrap.servers": os.getenv("KAFKA_URI", "localhost:9092")}
        self.files_path = [os.getenv("GPS_PATH"), os.getenv("INTERCEPTS_PATH"), os.getenv("IDENTITIES_PATH")]
        self.db_config = {
            "password": os.getenv("MYSQL_PASSWORD"),
            "host": os.getenv("MYSQL_HOST"),
            "db_name": os.getenv("MYSQL_DATABASE"),
            "user": os.getenv("MYSQL_USER")
        }