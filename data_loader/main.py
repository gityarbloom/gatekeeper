from project_data_preparer import DataPreparer
from project_config import ProjectConfig
from kafka_loader import KafkaProducer
from mysql_loader import MySqlLoader
import time



def run():
    print("\n\n🌞 --The gatekeeper start his action-- 🌞\n\n")
    config = ProjectConfig()
    data = DataPreparer(config.gps_path, config.intercepts_path, config.identities_path)
    
    prod = KafkaProducer(config.prod_config)
    prod.push_batch("gps_signals", data.gps_signals, 5)
    prod.push_batch("intercepts_signals", data.intercepts_signals, 5)

    suspects_data = data.create_suspects_df()
    accounts_financial = data.create_accounts_financial_df()

    mysql_loader = MySqlLoader(**config.db_config)
    mysql_loader.df_to_mysql(suspects_data, "suspects_data")
    mysql_loader.df_to_mysql(accounts_financial, "accounts_financial")
    time.sleep(60)
    print("\n\n🥱 --The gatekeeper finish his action-- 🥱\n\n")


if __name__ == "__main__":
    run()