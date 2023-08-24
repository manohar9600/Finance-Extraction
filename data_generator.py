# this file generates tags for each top3 table. by matching values from yahoo finance
import json
import re
import pandas as pd
from tqdm import tqdm
import extraction.gpt_functions as gpt_fns


tags_to_ignore = ["Reconciled Cost Of Revenue"]


def assign_tag_information(json_path, timeseries_path):
    tagged_tables = []
    with open(json_path, "r") as f:
        data = json.load(f)
    for table in data['tables']:
        if not table['class']:
            continue
        timeseries_table = pd.read_excel(timeseries_path, sheet_name=table['class'])
        table = get_tag_information(table, timeseries_table)
        if table != None:
            tagged_tables.append(table)
    
    if len(tagged_tables) == 0:
        return
    
    excel_path = json_path.replace("tables.json", "tagged_tables.xlsx")
    with pd.ExcelWriter(excel_path) as excel_writer:
        for table in tagged_tables:
            df = pd.DataFrame(table['body'])
            df.columns = table['header']
            df.to_excel(excel_writer, sheet_name=table['class'], index=False)
    return tagged_tables


def get_tag_information(table, timeseries_table):
    index, yr = get_latest_year_index(table['header'])
    timeseries_table_index = None
    for i, col in enumerate(timeseries_table.columns):
        if i != 0 and str(yr) in str(col):
            timeseries_table_index = i
            break
    else:
        return None
    new_table_body = []
    for i, row in enumerate(tqdm(table['body'], desc=table['class'], leave=False)):
        val = str(row[index])
        if not val or val == "0" or val == "0.0":
            new_table_body.append(row[:1] + [""] + row[1:])
            continue
        normalized_val = get_normalized_value(val, table['title'])
        tag, timeseries_table = get_tag(timeseries_table, timeseries_table_index, 
                      [normalized_val, val], row[0])
        new_table_body.append(row[:1] + [tag] + row[1:])
    table['header'] = table['header'][:1] + ['Tag'] + table['header'][1:]
    table['body'] = new_table_body
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
        try:
            yr = int(col.split()[-1])
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


if __name__ == "__main__":
    from glob import glob
    from utils import *
    for json_path in glob("data/current/*/*/tables.json"):
        if "timeseries" in json_path:
            continue
        print(f"processing file:{json_path}")
        cik = get_folder_names(json_path)[-3]
        timeseries_path = f"data/yahoofinance/{cik}/tables.xlsx"
        tables = assign_tag_information(json_path, timeseries_path)
    # with open(json_path.replace("tables.json", "tagged_tables.json"), "w") as f:
    #     json.dump({"tables": tables}, f, indent=4)