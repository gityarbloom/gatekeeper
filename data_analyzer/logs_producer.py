from confluent_kafka import Producer
from pathlib import Path
import time
import json



class LogsProducer:

    def __init__(self, prod_config):
        self.c_name = f"{Path(__file__).parent.name}: ".upper()
        self.config = prod_config
        self.prod = self.get_producer()


    def get_producer(self):
        for i in range(5):
            time.sleep(2)
            try:
                print("\nTry to connect to 'kafka'⏳...")
                prod = Producer(self.config)
                print("\n👍 Connected to 'kafka'!")
                return prod
            except Exception as e:
                print(f"\n👎 The {i+1} attempt of connection to 'Kafka' is Failed...")
                print(f"{e}")
                if i == 4:
                    raise Exception("\n❌ All The connection-attempts to 'Kafka' was Failed...")
                

    def publish_log(self, log):
        info_log = f"\n\n{self.c_name} :\n"
        try:
            self.prod.produce(topic="the-'gate_keeper'-logs", value=json.dumps(info_log).encode("utf-8"))
        except Exception:
            raise