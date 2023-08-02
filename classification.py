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
    if "notes to" in text.lower():
        return ""
    prompt = "does this heading belongs to any three important financial statements (Balance Sheet, Cash Flow, Income Statement). output none if it does not belong to any. Just output the category. don't explain."
    system_msg = f"You are a helpful assistant."
    
    # text = text.replace('\n', ' ')
    text = f'heading: "{text}"\n {prompt}'
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": text},
            ],
            temperature=0.2,
        )
        cls = response['choices'][0]['message']['content'].lower()
        if cls in ['balance sheet', 'income statement', 'cash flow']:
            return cls
    except:
        logger.info('failed to get response from openai api. sleeping for 5 secs')
        time.sleep(5)
        return check_table_class(text)
    return ''


def fix_table_class(cls, text):
    system_msg = f"Given a text, determine whether it represents the title of the {cls} or not."
    # text = text + f"\n---\nis this title of {cls}. tell me yes or no."
    prompt = f'Text: "{text}"\n Is given text contains title of the {cls}?. tell me yes or no.'
    # text = text.replace('\n', ' ')
    try:
        count = 0
        for _ in range(3):
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt},
                ],
                temperature=1,
            )
            if 'yes' in response['choices'][0]['message']['content'].lower()[:5]:
                count += 1
        if count == 3:
            return cls
    except:
        logger.info('failed to get response from openai api. sleeping for 5 secs')
        time.sleep(5)
        return fix_table_class(cls, text)
    return ''


def fix_table_classification(tables):
    for table in tqdm(tables):
        if table['class']:
            table['class'] = fix_table_class(table['class'], table['textAbove'])
    return tables


def classify_tables(tables):
    for i, table in enumerate(tqdm(tables)):
        if table['tableHTML'].count('</tr>') < 10:
            continue
        # # temporary fix for comprehensive income getting detected as income statement.
        # if i > 0 and tables[i-1]['class'] == 'income statement' and 'comprehensive income' in table['textAbove'].lower():
        #     continue
        table['class'] = get_table_class(table['textAbove'][-500:])
    return tables


def get_chatgpt_response(prompt):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        cls = response['choices'][0]['message']['content'].lower()
        return cls
    except:
        time_sleep = 2
        logger.info(f'failed to get response from openai api. sleeping for {time_sleep} secs')
        time.sleep(time_sleep)
        return get_chatgpt_response(prompt)


def get_table_class(text):
    if 'comprehensive income' in text.lower() or 'shareholders' in text.lower() or \
            'stockholders' in text.lower() or 'equity' in text.lower() or \
            'per common share' in text.lower():
        return ''
    prompt = f'text: "{text}"\ndoes this text belongs to any top3 financial statements?. return yes or no'
    top3_boolean = get_chatgpt_response(prompt)
    if 'no' in top3_boolean[:5]:
        return ''

    time.sleep(1)
    prompt = f'text: "{text}" to which class this text belongs. classes: balance sheet, cash flow, income statement. just return the class without explanation.'
    cls = get_chatgpt_response(prompt)
    if cls in ['balance sheet', 'income statement', 'cash flow']:
        return cls
    return ''


if __name__ == "__main__":
    for i in range(3):
        cls = get_table_class("CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME - USD ($) $ in Millions")
        print(cls)

