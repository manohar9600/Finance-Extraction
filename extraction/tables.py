import re
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
from utils import most_occuring_element


def construct_tables(tables):
    for table in tqdm(tables):
        if not table['class']:
            continue
        table['header'], table['body'] = extract_table_from_html(table['tableHTML'])
    return tables

def extract_table_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    rows = soup.find_all('tr')
    table_data = []

    for row in rows:
        if not row.get_text().strip():
            continue
        cells = row.find_all(['th', 'td'])
        row_data = []
        
        for cell in cells:
            colspan = int(cell.get('colspan', 1))
            # if a row contains single element, it will be centered to the table
            # which will be variable to financial table.
            if len(cells) == 1:
                colspan = 1
            row_data += [cell.get_text(separator=' ').strip()] * colspan
        table_data.append(row_data)
    
    columns, table_data = drop_unwanted_columns(table_data)
    cols_text = get_coumn_text(columns)

    return cols_text, table_data

def get_coumn_text(columns):
    if len(columns) == 0:
        return []
    cols_text = ['' for _ in range(len(columns[0]))]
    for row in columns:
        for i in range(len(row)):
            cols_text[i] = cols_text[i] + " " + row[i]
    for i in range(len(cols_text)):
        cols_text[i] = cols_text[i].strip()
    return cols_text

def drop_unwanted_columns(table):
    columns = [[] for _ in range(len(table[0]))]
    for i in range(len(table[0])):
        for row in table:
            if len(row) > i:
                columns[i].append(row[i])
            else:
                columns[i].append("")
    
    new_columns = []
    duplicates = []
    for i in range(len(columns)):
        if i in duplicates:
            continue
        for j in range(i, len(columns)):
            if j in duplicates:
                continue
            if columns[i] == columns[j]:
                duplicates.append(j)
        # new_columns += [columns[i]]
        new_columns = merge_columns(new_columns, columns[i])

    header_indice = get_column_indices(new_columns)    
    new_table = [[] for _ in range(len(new_columns[0]))]
    for i in range(len(new_columns[0])):
        for col in new_columns:
            new_table[i].append(col[i])

    header = new_table[:header_indice+1]
    new_table = new_table[header_indice+1:]

    return header, new_table

def merge_columns(column_lst, column2):
    if len(column_lst) == 0:
        return [column2]
    
    column1 = column_lst[-1]
    merge_boolean = True
    for i in range(len(column1)):
        if column1[i] != column2[i]:
            if re.search("[a-zA-Z0-9]", column1[i]) and re.search("[a-zA-Z0-9]", column2[i]):
                merge_boolean = False
                break
    if merge_boolean:
        for i in range(len(column1)):
            if column1[i] != column2[i]:
                column1[i] = column1[i] + column2[i]
    else:
        column_lst.append(column2)
    return column_lst

def get_column_indices(columns):
    num_cols = []
    for col in columns:
        full_text = "||".join(col)
        full_text = re.sub("[^a-zA-Z0-9|]", "", full_text)
        if re.search("\d{1,}\|\|\d{1,}\|\|\d{1,}", full_text):
            num_cols.append(col)

    col_indices = []
    for num_col in num_cols:
        col_ind = 0
        for i, val in enumerate(num_col):
            if re.search("[a-zA-Z]", val):
                col_ind += 1
                continue
            if re.search("\d{4}", val.strip()) and not "," in val:
                col_ind += 1
                continue
            if not val.strip():
                continue
            if re.search("\d{1,}", val):
                col_indices.append(col_ind-1)
                break
    
    col_ind = most_occuring_element(col_indices)[0]
    return col_ind


if __name__ == "__main__":
    import json
    table_json_path = r"data\a\000109087221000027\tables.json"
    with open(table_json_path, 'r') as f:
        data = json.load(f)
    tables = construct_tables(data['tables'])
    df = pd.DataFrame(tables[0]['body'])
    print(df)