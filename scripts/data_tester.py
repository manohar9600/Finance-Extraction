import os
import sys
import yaml
import pandas as pd
from loguru import logger
from tqdm import tqdm

current_script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_script_dir)
sys.path.append(parent_dir)
from extraction.db_functions import DBFunctions

YAHOO_DATA_FOLDER = "data\yahoofinance"


# class to test current db data with yahoo finance data.
class DBDataTester:
    def __init__(self) -> None:
        self.dbf = DBFunctions()
        with open("files\yahoo_vars_mapping.yaml", "r") as file:
            self.id_map = yaml.safe_load(file)  # mapping of db id with yahoo vars
        self.vars_df = self.dbf.get_table_data("variables")

    def process(self):
        logger.info("started analysing database")
        comp_df = self.dbf.get_table_data("companies")
        # TODO check value present in DB is valid or not.
        # TODO report variables in db and not present in yahoo.
        comparison_data = []
        for _, row in tqdm(comp_df.iterrows(), total=comp_df.shape[0]):
            comparison_data += self.process_company(row)
        self.calculate_stats(comparison_data)

    def process_company(self, company_info):
        values_df = self.dbf.get_table_data("values", {"companyid": company_info["id"]})
        if values_df.empty:
            return []
        yahoo_data = self.get_company_data_yahoo(company_info["Symbol"])
        comparison_data = []
        for _, row in values_df.iterrows():
            if row["variableid"] not in self.id_map:
                continue
            var_row = self.vars_df[self.vars_df["id"] == row["variableid"]].iloc[0]
            period_values = yahoo_data[var_row["table"]].get(self.id_map[row["variableid"]].lower(), {})
            ref_value = self.get_period_value(period_values, row["period"])
            comparison_data.append(
                [
                    var_row["variable"],
                    row["value"],
                    ref_value,
                    company_info["Symbol"],
                    row["period"],
                ]
            )
        return comparison_data

    def get_company_data_yahoo(self, symbol):
        file_path = os.path.join(YAHOO_DATA_FOLDER, str(symbol).lower(), "tables.xlsx")
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        data = {}
        for sheet_name in sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            table_data = {}
            for i, row in df.iloc[:, 1:].iterrows():
                table_data[df.iloc[i][0].lower()] = row.to_dict()
            data[sheet_name] = table_data
        return data

    def get_period_value(self, value_dict, period):
        # sometimes periods in db and yahoo won't match. this fn solves that.
        for key in value_dict:
            diff = period - key.date()
            if abs(diff.days) <= 30:
                return value_dict[key]
        return None

    def calculate_stats(self, comparison_data):
        for rec in comparison_data:
            if rec[2] is None:
                continue
            if rec[1] != rec[2]:
                self.log_msg(f"value mismatch. Variable: {rec[0]}, Company: {rec[3]}, Period: {rec[4]}, DB: {rec[1]}, YD: {rec[2]},")
    
    @staticmethod
    def log_msg(text):
        with open('report.txt', 'a') as f:
            f.write(text+'\n')

if __name__ == '__main__':
    with open('report.txt', 'w') as f:
        f.write('')

    DBDataTester().process()
