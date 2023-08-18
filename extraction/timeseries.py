import os
import re
import time
from glob import glob
import json
import openai
from loguru import logger


prompt_dict = {
    "income statement": """Please extract and calculate the following metrics from given balance sheet table. Output should be in markdown table format and column header should be periods and periods should be sorted in reverse.
1. Total Revenue
2. Cost of Goods Sold
3. Gross Profit
4. Operating Expenses
5. Operating Income
6. Pretax Income
7. Taxes
8. Net Income before Discontinued Operations
9. Net Income
10. Basic EPS
11. Diluted EPS
12. Average Basic Shares Outstanding
13. Diluted Shares Outstanding
14. EBITDA
15. EBIT
14. Total Expenses
---
table:

""",
    "balance sheet": """Please extract and calculate the following metrics from given balance sheet table. Output should be in markdown table format and column header should be periods and periods should be sorted in reverse.
1. Total Assets
2. Total Liabilities
3. Total Equity
4. Total liabilities & shareholders' equities
5. Common Stock Equity
6. Total Debt
7. Net Debt
---
table:

""",
    "cash flow": """Please extract and calculate the following metrics from given cash flow table. Output should be in markdown table format and column header should be periods and periods should be sorted in reverse.
1. Operating Cash Flow
2. Investing Cash Flow
3. Financing Cash Flow
4. Free Cash Flow
---
table:

"""
}


openai.api_key = "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"


def get_timeseries_data(folder_path):
    classfied_tables = {}
    for file in glob(os.path.join(folder_path, "*/tables.json")):
        if 'timeseries' in file.lower():
            continue
        with open(file, "r") as f:
            tables = json.load(f)['tables']
        for table in tables:
            if not table['class']:
                continue
            if table['class'] not in classfied_tables:
                classfied_tables[table['class']] = []
            classfied_tables[table['class']].append(table)
    
    timeseries_tables = []
    for cls in classfied_tables:
        timeseries_table = get_timeseries_table(classfied_tables[cls], cls)
        timeseries_tables.append(timeseries_table)
    return {
        "tables": timeseries_tables
    }


def get_timeseries_table(tables, cls):
    logger.info(f"generating time series for table:{cls}")
    final_table = []
    final_header = []
    for table in tables:
        table_markdown = convert_to_markdown(table['header'], table['body'])
        prompt = prompt_dict[cls] + table_markdown
        result = get_chatgpt_response(prompt)
        try:
            header, res_table = extract_table_markdown(result)
        except:
            continue
        if res_table[0][0] not in prompt.split("table:")[0]:
            logger.info("table came in reverse normalized format, inversing table")
            header, res_table = inverse_table(header, res_table)
        
        if len(final_table) == 0:
            for i in range(len(res_table)):
                final_table.append(res_table[i][:2])
            final_header = header[:2]
        else:
            for i in range(len(final_table)):
                final_table[i] = final_table[i] + res_table[i][1:2]
            final_header = final_header + header[1:2]
        logger.info(f"added column: {header[1]}")
    table_object = {
        "class": cls,
        "header": final_header,
        "body": final_table
    }
    return table_object


def convert_to_markdown(header, body):
    num_columns = len(body[0])
    header_row = "| " + " | ".join(map(str, header)) + " |"
    separator_row = "| " + " | ".join(["---"] * num_columns) + " |"
    body_rows = "\n".join("| " + " | ".join(map(str, row)) + " |" for row in body)
    markdown_table = header_row + "\n" + separator_row + "\n" + body_rows
    return markdown_table


def get_chatgpt_response(prompt, model="gpt-3.5-turbo"):
    try:
        response = openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are helpful assistant. Calcuate values if there is no direct value."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
        )
        cls = response['choices'][0]['message']['content']
        return cls
    except Exception as e:
        if "maximum context length" in e.user_message:
            model = "gpt-3.5-turbo-16k"
            logger.info(f'crossed token limit, trying with {model}')
        else:
            time_sleep = 2
            logger.info(f'failed to get response from openai api. sleeping for {time_sleep} secs')
            time.sleep(time_sleep)
        return get_chatgpt_response(prompt, model)


def extract_table_markdown(markdown_text):
    header = []
    table = []
    header_completed = False
    for line in markdown_text.split("\n"):
        if re.search("^\s{0,}\|.+\|$", line):
            rows = [cell.strip() for cell in line.split("|")[1:-1]]
            if not header_completed and \
                    re.search("\|\s{0,}-{3,}\s{0,}\|", line):
                header_completed = True
                continue
            if not header_completed:
                header.append(rows)
            else:
                table.append(rows)
        elif table:
            break
    return header[0], table


def inverse_table(header, res_table):
    new_header = [""]
    new_table = []
    for j in range(len(res_table[0])):
        if j != 0:
            row = [header[j]]
        for i in range(len(res_table)):
            if j == 0:
                new_header.append(res_table[i][j])
                continue
            row.append(res_table[i][j])
        if j != 0:
            new_table.append(row)
    return new_header, new_table


if __name__ == "__main__":
    folder_paths = list(glob("data/*"))
    for folder_path in folder_paths:
        os.makedirs(os.path.join(folder_path, 'timeseries'), exist_ok=True)
        file_path = os.path.join(folder_path, 'timeseries', 'tables.json')
        if os.path.exists(file_path):
            continue

        logger.info(f"processing folder: {folder_path}")
        result = get_timeseries_data(folder_path)
        with open(file_path, "w") as f:
            json.dump(result,f,indent=4)
