from confluent_kafka import Producer
import json



class KafkaLoader:
    def __init__(self, logger, config):
        self.logger = logger
        for retry in range(5):
            self.logger.publish_info_log("try to connect to kafka⏳...")
            try:
                self.prod = Producer(config)
                self.prod.list_topics(topic="gate_keeper", timeout=1)
                self.prod.flush(2)
                self.logger.publish_info_log("👍 connected to kafka!")
                break
            except Exception as e:
                self.logger.publish_err_log(f"👎 kafka-connection failed: {e}")
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
            self.logger.publish_err_log(f"👎 Delivery failed: {err}")
        else:
            self.logger.publish_info_log(f"👍 Delivered {msg.value().decode('utf-8')} \nto {msg.topic()}")