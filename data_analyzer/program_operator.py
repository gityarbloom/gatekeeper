from analyzer_config import AnalyzerConfig
from logs_producer import LogsProducer
from mongo_loader import MongoLoader
from mysql_analyzer import MySqlConnection
from kafka_consumer import KafkaConsumer
from processing_and_analysis import *



def play():
    config = AnalyzerConfig()
    logger = LogsProducer(config.prod)

    logger.publish_log("🌞 --The 'DATA-ANALYZERR' start his action-- 🌞")

    editor = TextEditor()
    consumer = KafkaConsumer(logger, config.consum, "gate_keeper")
    sql_db = MySqlConnection(logger **config.mysql)
    mongo_db = MongoLoader(logger, config.mongo)
    counter = 0

    for msg in consumer.consum():
        if not len(msg) == 5:
            continue
        counter +=1
        logger.publish_log(f"Received message: {msg}")
        logger.publish_log(f"***{counter}***")
        msg["text"] = editor.clean_html(msg["text"])
        msg["current_transaction_value"] = editor.price_extraction(msg["text"])
        risks = sql_db.get_risks(msg["suspect_id"])
        initial_risk = risks[0]
        credit_rating_factor = risks[1]
        if initial_risk > 7:
            credit_rating_factor = credit_rating_factor *1.5
        final_score = initial_risk +(msg["current_transaction_value"]/1000) *credit_rating_factor
        msg["initial_risk"] = initial_risk
        msg["credit_rating_factor"] = credit_rating_factor
        msg["final_score"] = final_score
        mongo_db.insert("gatekeeper_db", "gatekeeper_coll", [msg])