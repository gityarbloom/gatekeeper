from confluent_kafka import Producer
from datetime import datetime
from pathlib import Path
import time
import json



class LogsProducer:

    def __init__(self, prod_config):
        deco = "#"*25
        self.c_name = "\n\n" + deco + f"\n'{Path(__file__).parent.name}':\n".upper() + deco
        self.config = prod_config
        self.prod = self.get_producer()


    def get_producer(self):
        for i in range(5):
            time.sleep(2)
            try:
                print(f"{self.c_name}\nℹ️ 'INFO'\nTry to connect to 'kafka'⏳...")
                prod = Producer(self.config)
                prod.list_topics(topic = "the_gate_keeper_logs", timeout=1)
                prod.flush(2)
                print(f"{self.c_name}\nℹ️ 'INFO'\n👍 Connected to 'kafka'!")
                return prod
            except Exception as e:
                print(f"{self.c_name}\nℹ️ 'INFO'\n👎 The {i+1} attempt of connection to 'Kafka' is Failed...")
                if i == 4:
                    print(f"{e}")
                    raise Exception(f"{self.c_name}\n❌ 'ERROR'\n👎👎👎 All The connection-attempts to 'Kafka' was Failed...")
                

    def publish_info_log(self, info_log):
        timestamp = datetime.now().strftime("%d/%m/%Y--%H:%M:%S")
        log = f"{self.c_name} \nℹ️ 'INFO'\n⌚ {timestamp}\n{info_log}"
        try:
            self.prod.produce(topic="the_gate_keeper_logs", value=json.dumps(log).encode("utf-8"))
            self.prod.flush(2)
        except Exception:
            raise


    def publish_error_log(self, err_log):
        timestamp = datetime.now().strftime("%d/%m/%Y--%H:%M:%S")
        log = f"{self.c_name} \n❌ 'ERROR'\n⌚ {timestamp}\n{err_log}"
        try:
            self.prod.produce(topic="the_gate_keeper_logs", value=json.dumps(log).encode("utf-8"))
            self.prod.flush(2)
        except Exception:
            raise