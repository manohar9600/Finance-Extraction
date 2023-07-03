import json
import re

import bs4
import openai
from bs4 import BeautifulSoup
from openpyxl import Workbook
from tqdm import tqdm

MODEL = "gpt-3.5-turbo"
openai.api_key = "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"


def get_html_tables(html):
    soup = BeautifulSoup(html, 'html.parser')
    tables = []
    # Find all tables and extract their data into separate sheets
    previous_section = ''
    tables_html = soup.find_all('table')
    for table in tqdm(tables_html):
        # Find the text above the table up to the <hr> tag
        previous_elements = []
        prev_element = table.find_previous()
        while prev_element and prev_element.name != 'hr':
            if prev_element.name and len(prev_element.contents) == 1 and \
                    type(prev_element.contents[0]) != bs4.element.Tag:
                text = prev_element.get_text(strip=True)
                if text and text not in previous_elements:
                    previous_elements.append(text)
                if len(previous_elements) == 15:
                    break
            prev_element = prev_element.find_previous()

        # Reverse the list to maintain the original order of text
        previous_elements.reverse()

        # Add the extracted text to the sheet
        text = '\n'.join(previous_elements)

        # Extract table data
        table_rows = []
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            data = [cell.get_text(strip=True) for cell in cells]
            table_rows.append(data)

        section = re.search('Item \d{1}', text)
        if section is not None:
            section = section.group()
        else:
            section = previous_section
        
        tables.append({
            'class': '',
            'section': section,
            'textAbove': text.strip()[-500:],
            'header': [],
            'body': [],
            'tableHTML': table.decode_contents()
        })

        previous_section = section
    return tables


def check_table_class(text):
    prompt = "Is this contains financial statements heading. If yes, classify given text as BalanceSheet or IncomeStatement or Cashflow or None. don't explain."
    system_msg = f"You are a helpful assistant. Help me find right type of content in this pdf page. {prompt}"
        
    response = openai.ChatCompletion.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": text},
        ],
        temperature=0,
    )
    cls = response['choices'][0]['message']['content'].lower().replace(' ', '')
    if cls in ['balancesheet', 'incomestatement', 'cashflow']:
        return cls
    return ''


def classify_tables(tables):
    for table in tqdm(tables):
        # for sec documents, item 8 will have financial tables.
        if table['section'].lower() != 'item 8':
            continue
        table['class'] = check_table_class(table['textAbove'][-500:])
    return tables


def construct_proper_tables(tables):
    for table in tqdm(tables):
        if not table['class']:
            continue
        
        markdown_table = get_table_chatgpt(table)
        markdown_table = markdown_table.replace("**", "")
        rows = markdown_table.strip().split('\n')
        table['header'] = [cell.strip() for cell in rows[0].split('|')[1:-1]]
        table['body'] = [list(map(str.strip, row.split('|')[1:-1])) for row in rows[2:]]
    return tables
    

def get_table_chatgpt(table):
    soup = BeautifulSoup(table['tableHTML'], 'html.parser')
    table_text = soup.get_text(separator=" ")
    text = f"{table_text}\n---\nextract table from this text. output in markdown format. just give me only table"
    system_msg = f"You are a helpful assistant."
        
    response = openai.ChatCompletion.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": text},
        ],
        temperature=0,
    )
    content = response['choices'][0]['message']['content']
    return content


def save_to_excel(tables):
    # Create a new workbook
    workbook = Workbook()
    for i, table in enumerate(tables, start=1):
        if not len(table['class']):
            continue
        
        sheet = workbook.create_sheet(title=f"Table {i}")

        sheet['A1'] = table['class']
        for row in table['table']:
            sheet.append(row)
        
        # Remove the sheet if the table is empty
        if sheet.max_row == 1:
            workbook.remove(sheet)
        

    # Remove the default sheet created by Workbook()
    workbook.remove(workbook['Sheet'])

    # # Save the workbook to an Excel file
    workbook.save('output.xlsx')


if __name__ == "__main__":
    # Load the HTML file and create a BeautifulSoup object
    with open('aapl_sec.html', 'r') as file:
        html = file.read()

    tables = get_html_tables(html)
    tables = classify_tables(tables)
    tables = construct_proper_tables(tables)

    with open('debug.json', 'w') as f:
        json.dump(tables, f, indent=4)

    # save_to_excel(tables)
    print('done')

