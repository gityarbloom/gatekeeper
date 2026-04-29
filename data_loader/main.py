from logs_producer import LogsProducer
from data_preparer import DataPreparer
from loader_config import LoaderConfig
from kafka_loader import KafkaProducer
from mysql_loader import MySqlLoader
import time



def run():
    config = LoaderConfig()
    logger = LogsProducer(config.prod_config)

    logger.publish_info_log("*****🌞 --The DATA-LOADER start his action-- 🌞*****")

    data = DataPreparer(*config.files_path)
    prod = KafkaProducer(logger, config.prod_config)


    for j_path in config.files_path[:2]:
        prod.push_batch("gate_keeper", data.loading_json(j_path), 5)

    mysql_loader = MySqlLoader(logger, **config.db_config)
    table_names = mysql_loader.table_names
    suspects = data.create_suspects_df()
    accounts_financial = data.create_accounts_financial_df()
    df_tables = [suspects, accounts_financial]

    for i in range(len(df_tables)):
        mysql_loader.load_df_to_table(df_tables[i], table_names[i])
    time.sleep(60)
    logger.publish_info_log("*****🥱 --The DATA-LOADER finish his action-- 🥱*****")


if __name__ == "__main__":
    run()