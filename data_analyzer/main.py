from logs_producer import LogsProducer
from mysql import MySqlConnection
from config import AnalyzerConfig
from kafka import KafkaConsumer
from text import TextPreparer
from mongo import MongoLoader



def run():
    config = AnalyzerConfig()
    logger = LogsProducer(config.prod)
    logger.publish_info_log("🌞 --The 'DATA-ANALYZERR' start his action-- 🌞")

    editor = TextPreparer()
    consumer = KafkaConsumer(logger, config.consum, "gate_keeper")
    sql_db = MySqlConnection(logger, **config.mysql)
    mongo_db = MongoLoader(logger, config.mongo)
    counter1 = 0
    counter2 = 0

    for msg in consumer.consum():
        if len(msg) == 4:
            counter1 +=1
            logger.publish_info_log(f"A new 'GPS_STREAM' message \n**number {counter1}**\n is recived from 'Kafka'!")
            mongo_db.insert(counter1, "vehicle_id", "gatekeeper_db", "gps_data", msg)
        else:
            counter2 +=1
            logger.publish_info_log(f"A new 'INTERCEPTS' message \n**number {counter2}**\n is recived from 'Kafka'!")
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
            mongo_db.insert(counter2, "suspect_id", "gatekeeper_db", "intercepts", msg)




if __name__ == "__main__":
    run()