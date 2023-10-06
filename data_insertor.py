# file that reads files and inserts into postgres db
import os
import psycopg2
import json
import pandas as pd
from loguru import logger
from glob import glob
from datetime import datetime


def get_prod_variables():
    conn = psycopg2.connect(database="ProdDB",
                            host="localhost",
                            user="postgres",
                            password="manu1234",
                            port="5432")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM variables")
    df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
    conn.close()
    df = df[df['id'].isin([12,13])] # temporary line
    return df


def get_company_id(symbol):
    conn = psycopg2.connect(database="ProdDB",
                                host="localhost",
                                user="postgres",
                                password="manu1234",
                                port="5432")
    cursor = conn.cursor()
    cursor.execute(f'SELECT id FROM companies where "Symbol"=\'{symbol}\'')
    comp_id = cursor.fetchone()[0]
    conn.close()
    return comp_id


def get_time_diff(d1, d2):
    if not d1 and not d2:
        return None
    if not d1:
        diff = datetime.strptime(d2, '%Y-%m-%d') - datetime.min
        return diff.days
    if not d2:
        diff = datetime.min - datetime.strptime(d1, '%Y-%m-%d')
        return diff.days
    diff = datetime.strptime(d2, '%Y-%m-%d')-datetime.strptime(d1, '%Y-%m-%d')
    return diff.days


def get_xbrl_match(datapoint_values, var_dict, xbrlid):
    matches = []
    for dp in datapoint_values:
        if not "dimensions" in dp[2] and dp[2]["label"] == xbrlid:
            time_diff = get_time_diff(dp[2].get('start', ''), dp[2].get('endInstant', ''))
            if time_diff is not None and var_dict['table'] in ['income statement', 'cash flow'] and \
                    time_diff >= 360 and time_diff <= 370:
                matches.append(dp[2])
            elif time_diff is not None and var_dict['table'] == 'balance sheet' and \
                    time_diff >= 1000:
                matches.append(dp[2])

    final_match = None
    if matches:
        final_match = max(matches, key=lambda x:datetime.strptime(x['endInstant'], '%Y-%m-%d'))
    return final_match


def map_datapoint_values(datapoint_values, vars_df):
    datapoint_values = datapoint_values['factList']
    results = []
    for i, row in vars_df.iterrows():
        row = row.to_dict()
        for xbrlid in row['xbrlids']:
            match = get_xbrl_match(datapoint_values, row, xbrlid)
            row['match'] = match
            if match is not None:
                break
        results.append(row)
    return results


def insert_values(comp_sym, results):
    company_id = get_company_id(comp_sym)

    data_to_insert = []
    columns = ('value', 'scale', 'period', 'variableid', 'companyid', 'documenttype')
    for res in results:
        if not 'match' in res or res['match'] is None:
            continue
        data_to_insert.append([float(res['match']['value'].replace(',', '')), 1, 
                               datetime.strptime(res['match']['endInstant'], '%Y-%m-%d'),
                               res['id'], company_id, '10K'])
    
    conn = psycopg2.connect(database="ProdDB",
                            host="localhost",
                            user="postgres",
                            password="manu1234",
                            port="5432")
    cursor = conn.cursor()
    cursor.executemany(f"INSERT INTO values({','.join(columns)}) VALUES({','.join(['%s']*len(columns))})", data_to_insert)
    conn.commit()
    conn.close()
    logger.info('inserted values into database')


if __name__ == "__main__":
    vars_df = get_prod_variables()
    for folder in glob("data/current/*"):
        company_symbol = os.path.basename(folder)
        logger.info(f"processing company. symbol:{company_symbol}")
        for file_path in glob(os.path.join(folder, "*/xbrl_data.json")):
            with open(file_path, "r") as f:
                datapoint_values = json.load(f)
            if not 'factList' in datapoint_values:
                continue
            logger.info(f"processing file. {file_path}")
            results = map_datapoint_values(datapoint_values, vars_df)
            # insert_values(company_symbol, results)
