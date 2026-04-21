import pymysql
import time



class MySqlLoader:
    def __init__(self, password, host, db_name, user):
        self.conn = self.init_connection(password, host, user)
        self.init_db(db_name)
        self.table_names = self.create_tables()


    def init_connection(self, password, host, user):
        for retry in range(5):
            try:
                time.sleep(1)
                print("\nTry to connect to MySql⏳...")
                conn = pymysql.connect(password=password, host=host, user=user)
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                print(f"\n👍 Cnnected!")
                return conn
            except Exception as e:
                print(f"\n👎 Attempt {retry+1} failed: {e}")
                if retry == 4:
                    raise


    def init_db(self, db_name):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                cursor.execute(f"USE {db_name}")
            print(f"\n👍 Create n👍Database!\n")
            print(f"\n👍Database '{db_name}' is ready\n")
            self.conn.commit()
        except Exception as e:
            print(f"\n👎 Failed to initialize DB: {e}\n")
            raise


    def create_tables(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""CREATE TABLE IF NOT EXISTS suspects(
                    suspect_id VARCHAR(50) PRIMARY KEY,
                    full_name VARCHAR(255),
                    nationality VARCHAR(100),
                    last_known_location VARCHAR(255)
                )""")
                print(f"\nTable 'suspects' create succesfully\n")
                cursor.execute(f"""CREATE TABLE IF NOT EXISTS accounts_financial(
                    suspect_id VARCHAR(50),
                    bank_account VARCHAR(100),
                    initial_risk INTEGER,
                    credit_rating_factor INTEGER,
                    FOREIGN KEY (suspect_id) REFERENCES suspects(suspect_id)
                )""")
            print(f"\n👍 Table 'accounts_financial' create succesfully\n")
            self.conn.commit()
            return ["suspects", "accounts_financial"]
        except Exception as e:
            print(f"\n👎 Create tables failed... \n{e}\n")
            raise


    def load_df_to_table(self, df, table_name):
        try:
            data = df.values.tolist()
            cols = ", ".join([str(i) for i in df.columns.tolist()])
            placeholders = ", ".join(["%s"] * len(df.columns))
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            with self.conn.cursor() as cursor:
                cursor.executemany(sql, data)
            self.conn.commit()
            print(f"\nn👍 Successfully loaded {len(df)} rows to {table_name}")
        except Exception as e:
            self.conn.rollback()
            print(f"👎 Failed to load DF to {table_name}: {e}")
            raise