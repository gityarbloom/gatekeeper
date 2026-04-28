from confluent_kafka import Producer
from pathlib import Path
import time
import json



class LogsProducer:

    def __init__(self, prod_config):
        self.c_name = f"\n###'{Path(__file__).parent.name}'###: ".upper() + "\n"
        self.config = prod_config
        self.prod = self.get_producer()


    def get_producer(self):
        for i in range(5):
            time.sleep(2)
            try:
                print("\nTry to connect to 'kafka'⏳...")
                prod = Producer(self.config)
                prod.list_topics(topic = "the_gate_keeper_logs", timeout=1)
                prod.flush(2)
                print("\n👍 Connected to 'kafka'!")
                return prod
            except Exception as e:
                print(f"\n👎 The {i+1} attempt of connection to 'Kafka' is Failed...")
                print(f"{e}")
                if i == 4:
                    raise Exception("\n❌ All The connection-attempts to 'Kafka' was Failed...")
                

    def publish_log(self, log):
        info_log = f"\n\n{self.c_name} :\n{log}"
        try:
            
            self.prod.produce(topic="the_gate_keeper_logs", value=json.dumps(info_log).encode("utf-8"))
            self.prod.flush(2)
        except Exception:
            raise