import sys
import os
current_script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_script_dir)
sys.path.append(parent_dir)
import json
from extraction.process_excel import process_finance_excel





def generate_markdown_table(columns, table_body):
    header_row = "| " + " | ".join(columns) + " |"
    separator_row = "| " + " | ".join(['---'] * len(columns)) + " |"
    body_rows = ["| " + " | ".join(str(item) for item in row) + " |" for row in table_body]
    return "\n".join([header_row, separator_row] + body_rows)


if __name__ == "__main__":
    path = r"data\current\AFL\000000497723000055\afl-20221231.htm"
    excel_tables = process_finance_excel(os.path.join(os.path.dirname(path), 
                                                      'Financial_Report.xlsx'))
    with open(os.path.join(os.path.dirname(path), 'xbrl_pre.json')) as f:
        hierarchy_data = json.load(f)
    tags_list = get_table_tags_reported(hierarchy_data, top3_table_tags)
    excel_tables_dict = classify_tables_excel(excel_tables, tags_list)
    print('done')



    # get values using tag, match with excel, using text and value
    # can get text from arelle code.