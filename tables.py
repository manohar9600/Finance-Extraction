import re
from bs4 import BeautifulSoup
from openpyxl import Workbook
from tqdm import tqdm


def get_item8_tables(html):
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
            if prev_element.name and not "<table" in prev_element.decode_contents():
                text = prev_element.get_text(strip=True)
                if text not in previous_elements:
                    previous_elements.append(text)
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
        
        if section.lower() == 'item 8':
            tables.append({
                'section': section,
                'textAbove': text.strip(),
                'table': table_rows
            })

        previous_section = section

    return tables


# Load the HTML file and create a BeautifulSoup object
with open('0000320193-18-000145.html', 'r') as file:
    html = file.read()


import json
tables = get_item8_tables(html)



import openai
MODEL = "gpt-3.5-turbo"
openai.api_key = "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"


def check_table_class(text):
    prompt = "Classify given text as BalanceSheet or IncomeStatement or Cashflow or None. don't explain."
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

# Create a new workbook
workbook = Workbook()
for i, table in tqdm(enumerate(tables, start=1), total=len(tables)):
    sheet = workbook.create_sheet(title=f"Table {i}")
    table['class'] = check_table_class(table['textAbove'][-500:])
    if not len(table['class']):
        continue

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

with open('debug.json', 'w') as f:
    json.dump(tables, f, indent=4)

print('done')

