import time
import json
import re
import openai
from bs4 import BeautifulSoup
from openpyxl import Workbook
from tqdm import tqdm
from loguru import logger


openai.api_key = "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"


def get_sections(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    html_lines = html_content.split('\n')
    html_splits = soup.find_all('hr')
    if len(html_splits) == 0:
        return [html_content]
    
    sections = []
    prev_line = 0
    prev_pos = 0
    for i in range(len(html_splits)):
        if prev_line == html_splits[i].sourceline-1:
            sec_html = html_lines[prev_line][prev_pos:html_splits[i].sourcepos]
        else:
            sec_html = html_lines[prev_line][prev_pos:]
            sec_html += "\n".join(html_lines[prev_line+1:\
                                            html_splits[i].sourceline-1])
            sec_html += html_lines[html_splits[i].sourceline-1][:html_splits[i].sourcepos]
        sections.append(sec_html)
        prev_line = html_splits[i].sourceline-1
        prev_pos = html_splits[i].sourcepos
    else:
        i = len(html_splits)-1
        sec_html = html_lines[html_splits[i].sourceline-1][html_splits[i].sourcepos:]
        sec_html += "\n".join(html_lines[html_splits[i].sourceline:])

    return sections


def get_html_tables(html_content):
    sections = get_sections(html_content)
    previous_section = ''
    tables = []
    for section_html in tqdm(sections):
        soup = BeautifulSoup(section_html, 'html.parser')
        # cleaning table text for section text
        for _t in soup.find_all('table'):
            _t.string = ''
        section_text = soup.get_text(strip=True)
        matches = re.findall('item \d{1}', section_text.lower())
        if matches:
            section = matches[-1]
        else:
            section = previous_section
        _tables = get_tables(section_html)
        for tab in _tables:
            tab['section'] = section
        tables += _tables
        previous_section = section
    return tables


def get_tables(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    tables_html = soup.find_all('table')
    tables = []
    for table in tables_html:
        previous_elements = []
        for prev_element in table.find_all_previous(string = True, limit=50):
            prev_element = prev_element.replace("\n", " ").replace("\t", " ").strip()
            if prev_element:
                previous_elements.append(prev_element)
            if len(previous_elements) == 10: # limiting above text to 10 lines
                break
        
        previous_elements.reverse()
        text = '\n'.join(previous_elements)
        tables.append({
            'class': '',
            'section': '',
            'textAbove': text.strip(),
            'header': [],
            'body': [],
            'tableHTML': table.decode_contents()
        })
    return tables


def check_table_class(text):
    prompt = "Is this contains financial statements heading. If yes, classify given text as BalanceSheet or IncomeStatement or Cashflow or None. don't explain."
    system_msg = f"You are a helpful assistant. Help me find right type of content in this pdf page. {prompt}"
    
    text = text.replace('\n', ' ')
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": text},
            ],
            temperature=0,
        )
        cls = response['choices'][0]['message']['content'].lower().replace(' ', '')
        if cls in ['balancesheet', 'incomestatement', 'cashflow']:
            return cls
    except:
        logger.info('failed to get response from openai api. sleeping for 5 secs')
        time.sleep(5)
        return check_table_class(text)
    return ''


def classify_tables(tables):
    item8_exists = False
    for table in tables:
        if table['section'].lower() == 'item 8':
            item8_exists = True
    if not item8_exists:
        logger.info("Item 8 failed, falling to full classification")
    for table in tqdm(tables):
        # for sec documents, item 8 will have financial tables.
        if table['section'].lower() != 'item 8' and item8_exists:
            continue
        if table['tableHTML'].count('</tr>') < 10:
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
    
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": text},
            ],
            temperature=0,
        )
        content = response['choices'][0]['message']['content']
    except:
        logger.info('failed to get response from openai api. sleeping for 5 secs')
        time.sleep(5)
        return get_table_chatgpt(table)
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
    with open(r'data\abt\000104746918000856\a2234264z10-k.htm', 'r') as file:
        html = file.read()

    tables = get_html_tables(html)
    # tables = classify_tables(tables)
    # tables = construct_proper_tables(tables)

    with open('debug.json', 'w') as f:
        json.dump(tables, f, indent=4)

    # save_to_excel(tables)
    print('done')

