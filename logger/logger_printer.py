from confluent_kafka import Consumer, KafkaError
import time
import json



class LoggerPrinter:

    def __init__(self, consum_config):
        self.config = consum_config
        self.cons = self.get_consumer()


    def get_consumer(self):
        for i in range(5):
            time.sleep(2)
            try:
                print("\n\nThe 'LOGGER SERVICE' Try to connect to 'kafka'⏳...")
                cons = Consumer(self.config)
                print("\n👍 The 'LOGGER SERVICE' is  Connected to 'kafka'!")
                return cons
            except Exception as e:
                print(f"\n👎 The {i+1} attempt of connection the 'LOGGER SERVICE' to 'Kafka' is Failed...")
                print(f"\n{e}")
                if i == 4:
                    raise Exception("\n❌ All The connection-attempts between 'LOGGER SERVICE' & 'Kafka' Failed...")
                

    def consum_and_print_logs(self):
        self.cons.subscribe(["the-'gate_keeper'-logs"])
        print("\n\nThe 'LOGGER SERVICE' is Waiting for messages from KRaft cluster ⏳...")
        while True:
            try:
                msg = self.cons.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise f"\nError: {msg.error()}\n"
                print(json.loads(msg.value().decode('utf-8')))
                time.sleep(0.75)
            except Exception as e:
                print(f"\nconsumer failed because: \n{e}\n")
                raise
            finally:
                self.cons.close()