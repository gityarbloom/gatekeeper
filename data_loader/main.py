from project_data_preparer import DataPreparer
from project_config import ProjectConfig
from kafka_loader import KafkaProducer
from mysql_loader import MySqlLoader
import time



def run():
    print("\n\n🌞 --The DATA-LOADER start his action-- 🌞\n\n")
    config = ProjectConfig()
    data = DataPreparer(config.gps_path, config.intercepts_path, config.identities_path)
    
    prod = KafkaProducer(config.prod_config)
    prod.push_batch("gps_signals", data.gps_signals, 5)
    prod.push_batch("intercepts_signals", data.intercepts_signals, 5)

    suspects = data.create_suspects_df()
    accounts_financial = data.create_accounts_financial_df()

    mysql_loader = MySqlLoader(**config.db_config)
    table_names = mysql_loader.create_tables()
    mysql_loader.load_df_to_table(suspects, table_names.index("suspects"))
    mysql_loader.load_df_to_table(accounts_financial, table_names.index("accounts_financial"))
    time.sleep(60)
    print("\n\n🥱 --The DATA-LOADER finish his action-- 🥱\n\n")


if __name__ == "__main__":
    run()