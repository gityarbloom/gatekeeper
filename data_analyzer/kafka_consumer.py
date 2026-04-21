from confluent_kafka import Consumer, KafkaError
import os



consum_conf = {
    'bootstrap.servers': os.getenv("CONSUM_CONFIG", "localhost:9092"),
    'group.id': 'gatekeeper',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consum_conf)
consumer.subscribe(['my_topic'])

print("\nWaiting for messages from KRaft cluster...\n")

def kafka_consuming():
    try:
        while True:
            msg = consumer.poll(2)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"\nError: {msg.error()}\n")
                    break
            massage = msg.value().decode('utf-8')
            print(f"\nReceived message: {massage}\n")
            yield massage
    except Exception as e:
        print(f"\nconsumer failed because: \n{e}\n")
        raise
    finally:
        consumer.close()