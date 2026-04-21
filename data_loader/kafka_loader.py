from confluent_kafka import Producer
import json
import time


class KafkaProducer:
    def __init__(self, config):
        for retry in range(5):
            time.sleep(2)
            print("try to connect to kafka⏳...")
            try:
                self.prod = Producer(config)
                self.prod.list_topics(timeout=1)
                self.prod.flush(2)
                print("\n👍 connected to kafka!")
                break
            except Exception as e:
                print(f"👎 kafka-connection failed: {e}")
                if retry == 4:
                    raise


    def push_one(self, topic_name:str, value, flush=True):
        try:
            self.prod.produce(topic_name, json.dumps(value), callback=self.sending_report)
            if flush:
                self.prod.flush(1)
        except Exception:
            raise
            

    def push_batch(self, topic_name:str, data:list, batch_size:int):
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            for val in batch:
                self.push_one(topic_name, val, flush=False)
            self.prod.flush(batch_size)

    def sending_report(self, err, msg):
        if err:
            print(f"\n👎 Delivery failed: {err}")
        else:
            print(f"\n👍 Delivered {msg.value().decode('utf-8')} \nto {msg.topic()}")