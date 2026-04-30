from confluent_kafka import Consumer, KafkaError
from pathlib import Path
import time
import json



class LoggerConsumer:

    def __init__(self, consum_config):
        self.c_name = f"\n###'{Path(__file__).parent.name}'###: ".upper() + "\n"
        self.config = consum_config
        self.cons = self.get_consumer()


    def get_consumer(self):
        for i in range(5):
            time.sleep(2)
            try:
                print(f"{self.c_name}Try to connect to 'kafka'⏳...")
                cons = Consumer(self.config)
                print(f"{self.c_name}👍Connected to 'kafka'!")
                return cons
            except Exception as e:
                print(f"{self.c_name}👎 The {i+1} attempt of connection to 'Kafka' is Failed...")
                print(f"{self.c_name}{e}")
                if i == 4:
                    raise Exception(f"{self.c_name}❌ All The connection-attempts to 'Kafka' Failed...")
                

    def consum_logs(self):
        self.cons.subscribe(["the_gate_keeper_logs"])
        print(f"{self.c_name}Waiting for messages from KRaft cluster ⏳...")
        try:
            while True:
                msg = self.cons.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise Exception(f"{self.c_name}Error: {msg.error()}\n")
                print(json.loads(msg.value().decode('utf-8')))
                yield {"log": json.loads(msg.value().decode('utf-8'))}
                time.sleep(0.75)
        except Exception as e:
            print(f"{self.c_name}Consumer failed because: \n{e}\n")
            raise
        finally:
            self.cons.close()