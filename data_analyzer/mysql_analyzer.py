import pymysql
import time



class MySqlConnection:
    def __init__(self, logger, password, host, db_name, user):
        self.logger = logger
        self.conn = self.init_db_connection(password, host, db_name, user)


    def init_db_connection(self, password, host, db_name, user):
        for retry in range(5):
            try:
                time.sleep(2)
                self.logger.publish_info_log("Try to connect to 'MySql'⏳...")
                conn = pymysql.connect(password=password, host=host, user=user, database=db_name)
                self.logger.publish_info_log("👍 Cnnected to 'MySql'!")
                return conn
            except Exception as e:
                self.logger.publish_err_log(f"👎 mysql-connection-attempt number {retry+1} failed: {e}")
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