# this file generates tags for each top3 table. by matching values from yahoo finance
import json
import re
import pandas as pd
from tqdm import tqdm
import openpyxl
from utils import write_to_excel
import extraction.gpt_functions as gpt_fns


def process(json_path, timeseries_path):
    tagged_tables = []
    tables = get_present_tagged_data(json_path)
    for table in tables:
        if not table['class']:
            continue
        table['header'][0] = table['title']
        timeseries_table = pd.read_excel(timeseries_path, sheet_name=table['class'])
        timeseries_table = filter_tags(timeseries_table, table['class'])
        table = assign_tag_information(table, timeseries_table)
        if table != None:
            tagged_tables.append(table)
    
    if len(tagged_tables) == 0:
        return
    
    excel_path = json_path.replace("tables.json", "tagged_tables.xlsx")
    write_to_excel(tagged_tables, excel_path)
    return tagged_tables


def get_present_tagged_data(json_path):
    tables = []
    excel_path = json_path.replace("tables.json", "tagged_tables.xlsx")
    if os.path.exists(excel_path):
        workbook = openpyxl.load_workbook(excel_path)
        for t_name in workbook.sheetnames:
            table = {}
            df = pd.read_excel(excel_path, sheet_name=t_name)
            table['title'] = df.columns.tolist()[0]
            table['class'] = t_name.split("_")[-1]
            table['header'] = df.columns.tolist()
            table['body'] = df.values.tolist()
            tables.append(table)
    else:
        with open(json_path, "r") as f:
            data = json.load(f)
        tables = data['tables']
        for table in tables:
            new_table_body = []
            for row in table['body']:
                new_table_body.append(row[:1] + [""] + row[1:])
            table['header'] = table['header'][:1] + ['Tag'] + table['header'][1:]
            table['body'] = new_table_body
    return tables


def assign_tag_information(table, timeseries_table):
    index, yr = get_latest_year_index(table['header'])
    timeseries_table_index = None
    for i, col in enumerate(timeseries_table.columns):
        if i != 0 and str(yr) in str(col):
            timeseries_table_index = i
            break
    else:
        return None
    # new_table_body = []
    for i, row in enumerate(tqdm(table['body'], desc=table['class'], leave=False)):
        val = str(row[index])
        if not val or val == "0" or val == "0.0":
            # new_table_body.append(row[:1] + [""] + row[1:])
            continue
        if not pd.isna(row[1]) and len(row[1]) != 0: # ignoring already tagged variables.
            continue
        normalized_val = get_normalized_value(val, table['title'])
        tag, timeseries_table = get_tag(timeseries_table, timeseries_table_index, 
                      [normalized_val, val], row[0])
        row[1] = tag
        # new_table_body.append(row[:1] + [tag] + row[1:])
    # table['header'] = table['header'][:1] + ['Tag'] + table['header'][1:]
    # table['body'] = new_table_body
    return table


def get_tag(timeseries_table, timeseries_table_index, val_lst, variable):
    def clean_extra_zero(num):
        num_str = str(num)
        num_str = re.sub("\.0{1,2}$", "", num_str)
        return num_str
    
    tag = ''
    matches = []
    for df_ind, df_row in timeseries_table.iterrows():
        if clean_extra_zero(df_row[timeseries_table_index]) == val_lst[0] or \
                    clean_extra_zero(df_row[timeseries_table_index]) == val_lst[1]:
            # if not gpt_fns.check_gpt_match(variable, df_row[0]):
            #     continue
            
            tag = df_row[0]
            timeseries_table = timeseries_table.drop([df_ind])
            # matches.append([df_ind, tag])
            break
    # print("dd")
    return tag, timeseries_table


def get_latest_year_index(headers):
    years = []
    for col in headers:
        if 'months ended' in col.lower() and not '12 months ended' in col.lower():
            years.append(-1)
            continue
        try:
            yr = int(col.split()[-1])
            if 'jan' in col.lower() or 'january' in col.lower():
                yr = yr - 1
        except:
            yr = -1
        years.append(yr)

    max_index = years.index(max(years))
    return max_index, max(years)


def get_normalized_value(value_str, table_title):
    unit_strings = {
        "millions": "000000",
        "thousands": "000"
    }
    value_str = re.sub("[^\d.-]", "", value_str).strip()
    for unit in unit_strings:
        if unit in table_title.split()[-1].lower():
            value_str = value_str + unit_strings[unit]
            break
    return value_str


def filter_tags(timeseries_table, table_cls):
    # Gross Profit 
    tags_to_consider = {
        "income statement": [
            "Total Revenue", "Pretax Income", "Tax Provision", 
            "Net Income", "Basic EPS", "Diluted EPS",
        ],
        "balance sheet": [
            "Current Assets", "Total Non Current Assets", "Total Assets", "Current Liabilities",
            "Total Non Current Liabilities Net Minority Interest", 
            "Total Liabilities Net Minority Interest", "Total Equity Gross Minority Interest",
            "Common Stock Equity"
        ],
        "cash flow": [
            "Operating Cash Flow", "Investing Cash Flow", "Financing Cash Flow", 
            "Changes In Cash", "Capital Expenditure", "Free Cash Flow"
        ]
    }
    tags_to_consider = tags_to_consider[table_cls]
    indices_to_drop = []
    for i, row in timeseries_table.iterrows():
        if row[0] not in tags_to_consider:
            indices_to_drop.append(i)
    
    timeseries_table = timeseries_table.drop(indices_to_drop)
    return timeseries_table


if __name__ == "__main__":
    from glob import glob
    from utils import *
    paths = list(glob("data/current/*/*/tables.json"))
    paths = ["C:\\Users\\Manohar\\Desktop\\Projects\\Finance-Extraction\\data\\current\\aap\\000115844921000036\\tables.json"]
    for json_path in paths:
        if "timeseries" in json_path:
            continue
        print(f"processing file:{json_path}")
        cik = get_folder_names(json_path)[-3]
        timeseries_path = f"data/yahoofinance/{cik}/tables.xlsx"
        tables = process(json_path, timeseries_path)
    # with open(json_path.replace("tables.json", "tagged_tables.json"), "w") as f:
    #     json.dump({"tables": tables}, f, indent=4)