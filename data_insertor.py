# this file contains functions that can do xbrl search, xbrl calculcations
# fetching values using gpt for missing values
import os
import psycopg2
import pandas as pd
from loguru import logger
from datetime import datetime
import re
from extraction.utils import convert_to_number
from extraction.process_excel import process_finance_excel, classify_tables_excel
from extraction.gpt_functions import get_value_gpt


def get_prod_variables():
    conn = psycopg2.connect(
        database="ProdDB",
        host="localhost",
        user="postgres",
        password="manu1234",
        port="5432",
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM variables")
    df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
    conn.close()
    # df = df[df['id'].isin([1])] # temporary line
    return df


def get_time_diff(d1, d2):
    if not d1 and not d2:
        return None
    if not d1:
        diff = datetime.strptime(d2, "%Y-%m-%d") - datetime.min
        return diff.days
    if not d2:
        diff = datetime.min - datetime.strptime(d1, "%Y-%m-%d")
        return diff.days
    diff = datetime.strptime(d2, "%Y-%m-%d") - datetime.strptime(d1, "%Y-%m-%d")
    return diff.days


class DataInsertor:
    def __init__(self, symbol, xbrl_data, hierarchy) -> None:
        self.symbol = symbol.upper()
        self.company_id = self.get_company_id()
        self.xbrl_data = xbrl_data["factList"]
        self.hierarchy = hierarchy
        self.document_period = self.get_xbrl_match(None, "dei:DocumentPeriodEndDate")["value"]
        self.company_values = self.get_company_values() # contains company data of document_period
    
    def get_company_id(self):
        conn = psycopg2.connect(
            database="ProdDB",
            host="localhost",
            user="postgres",
            password="manu1234",
            port="5432",
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT id FROM companies where \"Symbol\"='{self.symbol}'")
        comp_id = cursor.fetchone()[0]
        conn.close()
        return comp_id

    def map_datapoint_values(self, vars_df, folder_path):
        results = []
        for _, row in vars_df.iterrows():
            row = row.to_dict()
            match = self.is_record_exists(row)
            for xbrlid in row["xbrlids"]:
                if match is None:
                    match = self.get_xbrl_match(row, xbrlid, self.document_period)
                if match is None:
                    match = self.get_formula_match(row, xbrlid)
                if match is not None:
                    break
            row["match"] = match
            results.append(row)

        results = self.calculate_missing_values(results, folder_path)
        results = self.calculate_variable_formulas(results)
        return results

    def get_xbrl_match(self, var_dict, xbrlid, document_period=None):
        matches = []
        for dp in self.xbrl_data:
            if not "dimensions" in dp[2] and dp[2]["label"] == xbrlid:
                time_diff = get_time_diff(dp[2].get("start", ""), dp[2].get("endInstant", ""))
                if var_dict is None:
                    matches.append(dp[2])
                elif (
                    time_diff is not None
                    and var_dict["table"] in ["income statement", "cash flow"]
                    and time_diff >= 360
                    and time_diff <= 370
                ):
                    matches.append(dp[2])
                elif (
                    time_diff is not None
                    and var_dict["table"] == "balance sheet"
                    and time_diff >= 1000
                ):
                    matches.append(dp[2])

        if matches:
            if document_period is None:
                best_match = max(
                    matches,
                    key=lambda x: datetime.strptime(x["endInstant"], "%Y-%m-%d"),
                )
                best_match["extraction"] = "extracted"
                return best_match
            else:
                for match in matches:
                    if match["endInstant"] == document_period:
                        match["extraction"] = "extracted"
                        return match
        return None

    def get_formula_match(self, row, xbrlid):
        def _find_match(obj, xbrlid):
            if obj[1]["label"] == xbrlid:
                return obj
            for _obj in obj[3:]:
                match = _find_match(_obj, xbrlid)
                if match is not None:
                    return match
            return None

        def _get_calculated_value(obj):
            sub_item_values = []
            for sub_item in obj[3:]:
                sub_item_match = self.get_xbrl_match(
                    row, sub_item[1]["label"], self.document_period
                )
                if sub_item_match is None:
                    sub_item_value = _get_calculated_value(sub_item)
                else:
                    sub_item_value = sub_item_match["value"]
                if sub_item_value is None:
                    return None
                sub_item_values.append(sub_item_value)
            if sub_item_values:
                return sum([convert_to_number(s) for s in sub_item_values])
            return None

        hierarchy = self.hierarchy["presentationLinkbase"]
        match = None
        top3_tables = [
            "us-gaap:IncomeStatementAbstract",
            "us-gaap:StatementOfFinancialPositionAbstract",
            "us-gaap:StatementOfCashFlowsAbstract",
        ]
        for table in hierarchy:
            table_identifier = table[3][1]["name"]
            if table_identifier not in top3_tables:
                continue
            match = _find_match(table[3], xbrlid + "Abstract")
            if match is not None:
                break
        if match is None or len(match) <= 3:
            return None
        value = _get_calculated_value(match)
        if value is None:
            return None
        finalres = {
            "label": xbrlid,
            "value": str(value),
            "endInstant": self.document_period,
            "extraction": "calculated",
        }
        return finalres

    def calculate_missing_values(self, results, folder_path):
        excel_tables = process_finance_excel(os.path.join(folder_path, "Financial_Report.xlsx"))
        excel_tables_dict = classify_tables_excel(excel_tables, self.hierarchy)
        for variable in results:
            # ignoring formula variables, this fn focusing only on base values
            if variable["formula"] is not None or variable.get("match", None) is not None:
                continue
            value = get_value_gpt(
                variable, self.document_period, excel_tables_dict.get(variable["table"], [])
            )
            if value is None or not value:
                continue
            finalres = {
                "label": "",
                "value": str(value),
                "endInstant": self.document_period,
                "extraction": "gpt",
            }
            variable["match"] = finalres
            logger.debug("gpt value:" + variable["variable"] + " " + str(value))
        return results

    def calculate_variable_formulas(self, results):
        # this function caluculates values by using formula mentioned in database
        # supports only additions and subtractions
        values_dict = {}
        for variable in results:
            if variable.get("match", None) is not None:
                values_dict[str(variable["id"])] = variable["match"]["value"]

        inter_dependency_boolean = False
        dependent_ids = (
            []
        )  # list to track sub ids, if any empty id which is required is calculated after then this function repeats.
        for variable in results:
            if variable["formula"] is None or variable.get("match", None) is not None:
                continue
            for formula in variable["formula"].split("|"):
                calculated_value = self.calculate_formula(formula, values_dict)
                if calculated_value is None:
                    continue
                finalres = {
                    "label": "",
                    "value": str(calculated_value),
                    "endInstant": self.document_period,
                    "extraction": "calculated",
                }
                variable["match"] = finalres
                logger.debug("calculated: " + variable["variable"] + " " + str(calculated_value))
                if not inter_dependency_boolean and str(variable["id"]) in dependent_ids:
                    inter_dependency_boolean = True
                break
            else:
                dependent_ids += re.findall("\d+", variable["formula"])

        if inter_dependency_boolean:
            logger.debug(
                "inter-dependency among formulas detected. re-calling calculation function"
            )
            results = self.calculate_variable_formulas(results)
        return results

    def calculate_formula(self, formula, values_dict):
        calculated_value = 0
        for var in re.findall(r"[+-]?\d+", formula):
            value = values_dict.get(var.replace("+", "").replace("-", ""), None)
            if value is None:
                return None
            if "-" in var:
                calculated_value -= convert_to_number(value)
            else:
                calculated_value += convert_to_number(value)
        return calculated_value

    def is_record_exists(self, res) -> dict:
        var_row = self.company_values[self.company_values['variableid'] == res['id']]
        if not var_row.empty:
            return {'value': var_row.iloc[-1]['value']}
        return None

    def get_company_values(self) -> pd.DataFrame:
        conn = psycopg2.connect(
            database="ProdDB",
            host="localhost",
            user="postgres",
            password="manu1234",
            port="5432",
        )
        cursor = conn.cursor()
        cursor.execute(
            f"select * from values where period='{datetime.strptime(self.document_period, '%Y-%m-%d')}' and companyid='{self.company_id}' and documenttype='10K'"
        )
        df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
        return df 

    def insert_values(self, results):
        data_to_insert = []
        columns = (
            "value",
            "scale",
            "period",
            "variableid",
            "companyid",
            "documenttype",
            "type",
        )
        for res in results:
            if res.get("match", None) is None or self.is_record_exists(res):
                continue
            data_to_insert.append(
                [
                    float(res["match"]["value"].replace(",", "")),
                    1,
                    datetime.strptime(res["match"]["endInstant"], "%Y-%m-%d"),
                    res["id"],
                    self.company_id,
                    "10K",
                    res["match"]["extraction"],
                ]
            )

        conn = psycopg2.connect(
            database="ProdDB",
            host="localhost",
            user="postgres",
            password="manu1234",
            port="5432",
        )
        cursor = conn.cursor()
        cursor.executemany(
            f"INSERT INTO values({','.join(columns)}) VALUES({','.join(['%s']*len(columns))})",
            data_to_insert,
        )
        conn.commit()
        conn.close()
        if data_to_insert:
            logger.info("inserted values into database")


# if __name__ == "__main__":
#     vars_df = get_prod_variables()
#     for folder in glob("data/current/*"):
#         company_symbol = os.path.basename(folder)
#         logger.info(f"processing company. symbol:{company_symbol}")
#         for file_path in glob(os.path.join(folder, "*/xbrl_data.json")):
#             with open(file_path, "r") as f:
#                 datapoint_values = json.load(f)
#             with open(file_path.replace("xbrl_data", "xbrl_pre"), "r") as f:
#                 hierarchy = json.load(f)
#             if not "factList" in datapoint_values:
#                 continue
#             logger.info(f"processing file. {file_path}")
#             results = map_datapoint_values(datapoint_values, vars_df, hierarchy)
#             insert_values(company_symbol, results)
