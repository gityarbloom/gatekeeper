import os


class ProjectConfig:
    def __init__(self):
        self.prod_config = {"bootstrap.servers": os.getenv("PROD_CONFIG", "localhost:9092")}
        self.gps_path = os.getenv("GPS_PATH")
        self.intercepts_path = os.getenv("INTERCEPTS_PATH")
        self.identities_path = os.getenv("IDENTITIES_PATH")
        self.db_config = {
            "password": os.getenv("MYSQL_PASSWORD"),
            "host": os.getenv("MYSQL_HOST"),
            "db_name": os.getenv("MYSQL_DATABASE"),
            "user": os.getenv("MYSQL_USER")
        }