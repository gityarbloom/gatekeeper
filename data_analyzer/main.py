from db_conn_and_searches import MySqlConnection
from kafka_consumer import KafkaConsumer
from processing_and_analysis import *
import os



print("\n\n🌞 --The DATA-ANALYZERR start his action-- 🌞\n\n")

db_config = {
            "password": os.getenv("MYSQL_PASSWORD"),
            "host": os.getenv("MYSQL_HOST"),
            "db_name": os.getenv("MYSQL_DATABASE"),
            "user": os.getenv("MYSQL_USER")
        }
topic_name = "Gate_Keeper"
consum_conf = {
            'bootstrap.servers': os.getenv("CONSUM_CONFIG"),
            'group.id': 'gatekeeper',
            'auto.offset.reset': 'earliest'
            }

editor = TextEditor()
consumer = KafkaConsumer(consum_conf, topic_name)
db = MySqlConnection(**db_config)

counter = 0
for msg in consumer.consum():
    counter += 1
    print(f"\n❗ A new message (number: {counter}) received")
    print(f"\n{msg}\n")
    msg["text"] = editor.clean_html(msg["text"])
    msg["value_transaction_current"] = editor.price_extraction(msg["text"])

print("\n\n🥱 --The DATA-ANALYZERR finshed his action-- 🥱\n\n")


ex = {
    "message_id": "M_001",
    "suspect_id": "S_103",
    "text": "The <b>package</b> will be delivered at <i>midnight</i> near point X. Use the rifle. Payment: $15000 on delivery.",
    "date": "2026-05-12T14:30:00",
    "tags": [
      "weapons",
      "urgent"
    ]
  }