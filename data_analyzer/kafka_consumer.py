from confluent_kafka import Consumer, KafkaError
import time
import json



class  KafkaConsumer:
    
    def __init__(self, logger, config:dict, topic_name:str):
        self.logger = logger
        self.config = config
        self.topic_name = topic_name
        self.consumer = None


    def init_consumer(self):
        for i in range(5):
            time.sleep(1)
            self.logger.publish_log("Try to connect to kafka⏳...")
            try:
                consumer = Consumer(self.config)
                self.logger.publish_log("👍 connected to 'Kafka'!")
                return consumer
            except Exception as e:
                if i == 4:
                    self.logger.publish_log(f"👎 Kafka-connection was faild \n{e}")
                    raise

    def consum(self):
        if not self.consumer:
            self.consumer = self.init_consumer()
            self.consumer.subscribe([self.topic_name])
        self.logger.publish_log("Waiting for messages from 'Kafka'...")
        try:
            while True:
                msg = self.consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self.logger.publish_log(f"Error: {msg.error()}")
                        break
                yield json.loads(msg.value().decode('utf-8'))
                time.sleep(1)
        except Exception as e:
            self.logger.publish_log(f"Consumer failed because: \n{e}")
            raise
        finally:
            self.consumer.close()