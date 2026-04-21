from sqlalchemy import create_engine
import pymysql
import time



class MySqlLoader:
    def __init__(self, password, host, db_name, user):
        self.init_db(password, host, db_name, user)
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db_name}")

    def init_db(self, password, host, db_name, user):
        for retry in range(5):
            try:
                time.sleep(1)
                print("\nTry to connect and create to MySql-DB⏳...")
                conn = pymysql.connect(password=password, host=host, user=user)
                with conn.cursor() as cursor:
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                conn.close()
                print(f"\n👍 Cnnected!")
                print(f"\n👍Database '{db_name}' is ready")
                break
            except Exception as e:
                print(f"\n👎 Attempt {retry+1} failed: {e}")
                if retry == 4:
                    raise

    def df_to_mysql(self, df, table_name:str, mode='replace'):
        df.to_sql(name=table_name, con=self.engine, if_exists=mode, index=False)
        print(f"\n☝️ Table '{table_name}' uploaded!")