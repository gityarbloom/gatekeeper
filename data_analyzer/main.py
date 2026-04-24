from mysql_analyzer import MySqlConnection
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
    print(f"\nReceived message: {msg}\n")
    if len(msg) == 5:
        msg["text"] = editor.clean_html(msg["text"])
        msg["current_transaction_value"] = editor.price_extraction(msg["text"])
        risks = db.get_risks(msg["suspect_id"])
        initial_risk = risks[0]
        credit_rating_factor = risks[1]
        if initial_risk > 7:
            credit_rating_factor = credit_rating_factor *1.5
        final_score = initial_risk +(msg["current_transaction_value"]/1000) *credit_rating_factor
        msg["initial_risk"] = initial_risk
        msg["credit_rating_factor"] = credit_rating_factor
        msg["final_score"] = final_score
        counter +=1
        print(f"\n\n***{counter}***")
        print(f"*************{msg}*************\n\n")
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








# os.getenv("MONGO_URI", "mongodb://localhost:27017/")