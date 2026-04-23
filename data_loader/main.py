from data_preparer import DataPreparer
from loader_config import LoaderConfig
from kafka_loader import KafkaProducer
from mysql_loader import MySqlLoader
import time



def run():
    print("\n\n🌞 --The DATA-LOADER start his action-- 🌞\n\n")

    config = LoaderConfig()
    data = DataPreparer(*config.files_path)
    prod = KafkaProducer(config.prod_config)


    for j_path in config.files_path[:2]:
        prod.push_batch("Gate_Keeper", data.loading_json(j_path), 5)

    mysql_loader = MySqlLoader(**config.db_config)
    table_names = mysql_loader.table_names
    suspects = data.create_suspects_df()
    accounts_financial = data.create_accounts_financial_df()
    df_tables = [suspects, accounts_financial]

    for i in range(len(df_tables)):
        mysql_loader.load_df_to_table(df_tables[i], table_names[i])
    time.sleep(60)
    print("\n\n🥱 --The DATA-LOADER finish his action-- 🥱\n\n")


if __name__ == "__main__":
    run()