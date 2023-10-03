# file to fetch data from wikipedia s&p data.
import os
import requests
import json
from tqdm import tqdm
from bs4 import BeautifulSoup
import psycopg2


def divide_column_rows(table_rows):
    column_index = 0
    for row in table_rows:
        non_variable_text = ""
        for _c in row.find_all('td')[1:]:
            if _c.find_all('ix:nonfraction'):
                break
            non_variable_text += _c.get_text()
        else:
            if column_index != 0 and not non_variable_text.strip() and row.get_text().strip():
                break
            column_index += 1
            continue
        break
    
    return table_rows[:column_index], table_rows[column_index:]


def get_company_name(cell):
    company_page = "https://en.wikipedia.org" + cell.find('a').get('href')
    r = requests.get(company_page)
    html_content = r.text
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup.find("h1", {"id": "firstHeading"}).get_text().strip('\n').strip()


def extract_html_table(table_tag):
    table = []
    table_rows = table_tag.find_all('tr')    
    for row in tqdm(table_rows[1:]): # adding last row of column header to get proper column coordinates
        table_cells = row.find_all('td')
        row = []
        for j, cell in enumerate(table_cells):
            row.append(cell.get_text().strip('\n').strip())
            if j == 1:
                company_name = get_company_name(cell)
                row.append(company_name)
        table.append(row)

    return table


def insert_values(results):
    data_to_insert = []
    columns = ('symbol', 'name', 'security', '"GICS sector"', '"GICS sub-industry"', '"headquarters location"')
    for res in results[0]:
        data_to_insert.append([res[0], res[2], res[1], res[3], res[4], res[5]])
    
    conn = psycopg2.connect(database="ProdDB",
                            host="localhost",
                            user="postgres",
                            password="manu1234",
                            port="5432")
    cursor = conn.cursor()
    cursor.executemany(f"INSERT INTO companies({','.join(columns)}) VALUES({','.join(['%s']*len(columns))})", data_to_insert)
    conn.commit()
    conn.close()


def extract_html_tables(html_content):
    # with open(html_path, 'r', encoding='utf-8') as f:
    soup = BeautifulSoup(html_content, 'html.parser')

    tables = []
    table_tags = soup.find_all('table')
    # for table_tag in table_tags:
    tables.append(extract_html_table(table_tags[0]))
    
    return tables    


if __name__ == "__main__":
    # folder_path = r"C:\Users\Manohar\Desktop\Projects\Finance-Extraction\data\xbrl-documents"
    # html_path = os.path.join(folder_path, "axp-20221231.htm")
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    r = requests.get(url)
    html_content = r.text

    data = extract_html_tables(html_content)
    insert_values(data)