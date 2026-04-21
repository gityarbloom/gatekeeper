import pandas as pd
import time
import json



class DataPreparer:
    def __init__(self, gps_path, intercepts_path, identities_path):
        self.gps_signals = self.loading_from_json(gps_path)
        time.sleep(2)
        self.intercepts_signals = self.loading_from_json(intercepts_path)
        time.sleep(2)
        self.df = self.loading_from_csv(identities_path)


    @staticmethod
    def loading_from_json(file_path:str, opening_way="r"):
        with open(file_path, opening_way, encoding='utf-8') as f:
            return json.load(f)
        

    @staticmethod
    def loading_from_csv(file_path:str):
        return pd.read_csv(file_path)
        

    def create_suspects_df(self):
        suspects_df = self.df[["suspect_id", "full_name", "nationality", "last_known_location"]]
        return suspects_df


    def create_accounts_financial_df(self):
        accounts_financial = self.df[["suspect_id", "bank_account", "initial_risk"]].copy()
        accounts_financial["credit_rating_factor"] = pd.cut(
            self.df["initial_risk"],
            bins=[0, 3, 6, 8, 10],
            labels=[1, 3, 4, 5]
        )
        return accounts_financial