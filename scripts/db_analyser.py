import psycopg2
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from glob import glob


# check for duplicates
# check for empty values
# check for missing dates
# check for non-numbers

def log_msg(text):
    with open('report.txt', 'a') as f:
        f.write(text+'\n')


def get_table_data(table_name):
    conn = psycopg2.connect(database="ProdDB",
                            host="localhost",
                            user="postgres",
                            password="manu1234",
                            port="5432")
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
    conn.close()
    return df


def check_value_existence(table_name, col_name, value):
    conn = psycopg2.connect(database="ProdDB",
                            host="localhost",
                            user="postgres",
                            password="manu1234",
                            port="5432")
    cursor = conn.cursor()
    cursor.execute(f"select * from {table_name} where {col_name}={value}")
    result = cursor.fetchone()
    if result:
        return True
    return False


def get_values_data(companyid):
    conn = psycopg2.connect(database="ProdDB",
                            host="localhost",
                            user="postgres",
                            password="manu1234",
                            port="5432")
    cursor = conn.cursor()
    cursor.execute(f"select * from values where companyid={companyid}")
    df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
    conn.close()
    return df


with open('report.txt', 'w') as f:
    f.write('')

variables_df = get_table_data('variables')
companies_df = get_table_data('companies')
# values_df = get_table_data('values')
non_empty_companies = []

for _, company_row in tqdm(companies_df.iterrows(), total=companies_df.shape[0]):
    # code for finding companies whose data is not present in database
    if not check_value_existence('values', 'companyid', company_row['id']):
        # log_msg(f'{company_row["Symbol"]} not present in database')
        continue

    non_empty_companies.append(company_row['Symbol'])
    # code for checking if there any file's data is not inserted into database or not.
    values_df = get_values_data(company_row['id'])
    periods = values_df['period'].unique()
    files_count = len(glob(f"data/current/{company_row['Symbol']}/*"))
    if len(periods) != files_count:
        log_msg(f'symbol:{company_row["Symbol"]} | count in-difference | files:{files_count}, DB: {len(periods)}')

    # code for checking missing or extra periods
    # if (datetime.now() - datetime.strptime("2023-" + company_row["Fiscal Year"], 
    #                                        "%Y-%m-%d")).days >= 30:
    #     dates_to_exist = [datetime.strptime(str(yr) + "-" + company_row["Fiscal Year"], 
    #                                     "%Y-%m-%d").date() for yr in range(2018, 2024)]
    # else:
    #     dates_to_exist = [datetime.strptime(str(yr) + "-" + company_row["Fiscal Year"], 
    #                                         "%Y-%m-%d").date() for yr in range(2017, 2023)]
    # for de in dates_to_exist:
    #     for pe in periods:
    #         diff = pe - de
    #         if abs(diff.days) < 10:
    #             break
    #     else:
    #         log_msg(f'symbol:{company_row["Symbol"]} | period:{datetime.strftime(de, "%Y-%m-%d")} | not present')
    
    # for pe in periods:
    #     for de in dates_to_exist:
    #         diff = pe - de
    #         if abs(diff.days) < 10:
    #             break
    #     else:
    #         log_msg(f'symbol:{company_row["Symbol"]} | period:{datetime.strftime(pe, "%Y-%m-%d")} | is extra')

    # code to find variables which values are empty
    empty_values = ''
    periods_set = set(values_df['period'])
    for varid, grp in values_df.groupby('variableid'):
        missing_values = periods_set - set(grp['period'])
        if missing_values:
            variable = variables_df[variables_df['id'] == varid].iloc[0]['variable']
            log_msg(f"variable:{variable} | symbol:{company_row['Symbol']} | periods:{[datetime.strftime(p, '%Y-%m-%d') for p in list(missing_values)]} | values are empty")
    
    variable_ids = values_df['variableid'].unique()
    for _, row in variables_df.iterrows():
        if row['id'] not in variable_ids:
            log_msg(f"variable:{row['variable']} | symbol:{company_row['Symbol']} | no values for this variable")

    # # code to find duplicate records
    # for varid, grp in values_df.groupby('variableid'):
    #     periods_count = grp['period'].value_counts().to_dict()
    #     for period in periods_count:
    #         if periods_count[period] > 1:
    #             unique_values_count = len(grp[grp['period'] == period]['value'].unique())
    #             variable = variables_df[variables_df['id'] == varid].iloc[0]['variable']
    #             period_str = datetime.strftime(period, '%Y-%m-%d')
    #             if unique_values_count > 1:
    #                 log_msg(f"variable:{variable} | symbol:{company_row['Symbol']} | period: {period_str} | duplicate data with different values")
                # else:
                #     log_msg(f"variable:{variable} | symbol:{company_row['Symbol']} | period: {period_str} | duplicate data with same values")


# print(non_empty_companies)