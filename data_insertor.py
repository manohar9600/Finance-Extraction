# file that reads files and inserts into postgres db
import os
import psycopg2
import json
import pandas as pd
from loguru import logger
from glob import glob
from datetime import datetime
import re
from extraction.utils import convert_to_number


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
    df = df[df['id'].isin([1])] # temporary line
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


def get_xbrl_match(datapoint_values, var_dict, xbrlid, document_period=None):
    matches = []
    for dp in datapoint_values:
        if not "dimensions" in dp[2] and dp[2]["label"] == xbrlid:
            time_diff = get_time_diff(dp[2].get('start', ''), dp[2].get('endInstant', ''))
            if var_dict is None:
                matches.append(dp[2])
            elif time_diff is not None and var_dict['table'] in ['income statement', 'cash flow'] and \
                    time_diff >= 360 and time_diff <= 370:
                matches.append(dp[2])
            elif time_diff is not None and var_dict['table'] == 'balance sheet' and \
                    time_diff >= 1000:
                matches.append(dp[2])

    if matches:
        if document_period is None:
            return max(matches, key=lambda x:datetime.strptime(x['endInstant'], '%Y-%m-%d'))
        else:
            for match in matches:
                if match['endInstant'] == document_period:
                    return match
    return None


def get_formula_match(datapoint_values, row, xbrlid, document_period, hierarchy):
    def _find_match(obj, xbrlid):
        if obj[1]['label'] == xbrlid:
            return obj
        for _obj in obj[3:]:
            match = _find_match(_obj, xbrlid)
            if match is not None:
                return match
        return None

    def _get_calculated_value(obj):
        sub_item_values = []
        for sub_item in obj[3:]:
            sub_item_match = get_xbrl_match(datapoint_values, row, 
                                        sub_item[1]['label'], document_period)
            if sub_item_match is None:
                sub_item_value = _get_calculated_value(sub_item)
            else:
                sub_item_value = sub_item_match['value']
            if sub_item_value is None:
                return None
            sub_item_values.append(sub_item_value)
        if sub_item_values:
            return sum([convert_to_number(s) for s in sub_item_values])
        return None

    hierarchy = hierarchy['presentationLinkbase']
    match = None
    top3_tables = ['us-gaap:IncomeStatementAbstract', 
                   'us-gaap:StatementOfFinancialPositionAbstract', 
                   'us-gaap:StatementOfCashFlowsAbstract']
    for table in hierarchy:
        table_identifier = table[3][1]['name']
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
        'label': xbrlid,
        'value': str(value),
        'endInstant': document_period
    }
    return finalres


def map_datapoint_values(datapoint_values, vars_df, hierarchy):
    datapoint_values = datapoint_values['factList']
    results = []
    empty_vars = []
    if not datapoint_values:
        return results
    document_period = get_xbrl_match(datapoint_values, None, 'dei:DocumentPeriodEndDate')['value']
    for i, row in vars_df.iterrows():
        row = row.to_dict()
        for xbrlid in row['xbrlids']:
            match = get_xbrl_match(datapoint_values, row, xbrlid, document_period)
            if match is None:
                match = get_formula_match(datapoint_values, row, xbrlid, document_period, hierarchy)
            row['match'] = match
            if match is not None:
                break
        if 'match' in row and row['match'] is not None:
            results.append(row)
        else:
            empty_vars.append(row)
    results = calculate_variable_formulas(results, empty_vars, document_period)
    return results


def calculate_variable_formulas(results, empty_vars, document_period):
    # this function caluculates values by using formula mentioned in database
    # supports only additions and subtractions
    values_dict = {}
    for var in results:
        values_dict[str(var['id'])] = var['match']['value']
    for variable in empty_vars:
        if variable['formula'] is None:
            continue
        calculated_value = 0
        for var in re.findall(r'[+-]?\d+', variable['formula']):
            value = values_dict.get(var.replace('+','').replace('-', ''), None)
            if value is None:
                break
            if "-" in var:
                calculated_value -= convert_to_number(value)
            else:
                calculated_value += convert_to_number(value)
        else:
            finalres = {
                'label': '',
                'value': str(calculated_value),
                'endInstant': document_period
            }
            variable['match'] = finalres
            results.append(variable)

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
            with open(file_path.replace("xbrl_data", "xbrl_pre"), "r") as f:
                hierarchy = json.load(f)
            if not 'factList' in datapoint_values:
                continue
            logger.info(f"processing file. {file_path}")
            results = map_datapoint_values(datapoint_values, vars_df, hierarchy)
            insert_values(company_symbol, results)