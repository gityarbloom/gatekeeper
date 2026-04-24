import pymysql
import time



class MySqlConnection:
    def __init__(self, password, host, db_name, user):
        self.conn = self.init_db_connection(password, host, db_name, user)


    def init_db_connection(self, password, host, db_name, user):
        for retry in range(5):
            try:
                time.sleep(2)
                print("\nTry to connect to MySql⏳...")
                conn = pymysql.connect(password=password, host=host, user=user, database=db_name)
                print(f"\n👍 Cnnected!")
                return conn
            except Exception as e:
                print(f"\n👎 Attempt {retry+1} failed: {e}")
                if retry == 4:
                    raise
    

    def get_risks(self, suspect_id:str):
        query = f"""SELECT initial_risk, credit_rating_factor
            FROM accounts_financial
            WHERE suspect_id = %s
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (suspect_id,))
        result = cursor.fetchone()
        cursor.close()
        return result