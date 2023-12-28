# this file started intend to check how system able to extract html tables
# logic based on comparing html tables with excel tables.
# to know which table to compare, tried xbrl presentation data.
# for excel tables classification, it is good. But unable to detect html tables classification.

import sys
import os
current_script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_script_dir)
sys.path.append(parent_dir)
import re
import json
from extraction.html_functions import extract_html_tables
from extraction.process_excel import process_finance_excel


top3_table_tags = ['us-gaap:IncomeStatementAbstract', 
                   'us-gaap:StatementOfFinancialPositionAbstract', 
                   'us-gaap:StatementOfCashFlowsAbstract']


def log_msg(text):
    with open('report.txt', 'a') as f:
        f.write(text+'\n')


def get_all_tags(head_list, tags):
    for obj in head_list:
        # if obj[2].get("type", "") == "Monetary":
        tags.append(obj[1]['label'])
        if len(obj[3:]) > 0:
            tags = get_all_tags(obj[3:], tags)
    return tags


def get_table_tags_reported(hierarchy_data, top3_table_tags):
    # code for fetching reported tags of each table. for now considering top3 tables.
    tags_list = []
    for table in hierarchy_data['presentationLinkbase']:
        if table[3][1]['label'] in top3_table_tags and \
            not 'Parenthetical' in table[1]['definition']:
            tags = []
            tags = get_all_tags(table[3][3:], tags)
        # if table[3][1]['label'] not in tags_dict:
        #     tags_dict[table[3][1]['label']] = []
        # tags_dict[table[3][1]['label']] += tags
            tags_list.append([table[3][1]['label'], 
                          table[1]['definition'].split('-')[-1].strip(), tags])
                              
    return tags_list


def get_current_date_contextref(xbrl_data):
    # this function fetches context ref, that is mentioned for document period data.
    # and to main tag, when there is multiple tags reported.
    context_ref = ''
    for entitity in xbrl_data['factList']:
        if entitity[1]['name'] == 'dei:EntityCentralIndexKey':
            context_ref = entitity[2]["contextRef"]
            break
    if context_ref == '':
        raise ValueError('failed to fetch context reference')
    return context_ref


def classify_tables_html(html_tables, tags_list, context_ref):
    # by using reported tags of a table, trying to classify html table into it.
    html_tables_dict = {}
    for table in html_tables[23:]:
        for table_tags in tags_list:
        # table_tags = tags_dict[table_tag]
            match_count = 0
            total_count = 0
         # maintaining this variable to check only unique tags for table classification match
            completed_tags = []
            for row in table['body']:
                if not row[1]:
                    continue
                tag, tag_context_ref = row[1]
                if len(tag) == 0 or not re.search("\d", "".join(row[2:])) or \
                    tag in completed_tags or context_ref not in tag_context_ref:
                    continue
                if tag in table_tags[2]:
                    match_count += 1
                    completed_tags.append(tag)
                total_count += 1
        
            match_perc = match_count / max(1, total_count)
            if total_count > 5 and match_perc >= 0.8:
                if table_tags[0] not in html_tables_dict:
                    html_tables_dict[table_tags[0]] = []
                html_tables_dict[table_tags[0]].append(table)
                break
    return html_tables_dict


def classify_tables_excel(excel_tables, tags_list):
    excel_tables_dict = {}
    for table in excel_tables:
        for table_tags in tags_list:
            if table_tags[1] in table['title'] and not '(Parentheticals)' in table['title']:
                if table_tags[0] not in excel_tables_dict:
                    excel_tables_dict[table_tags[0]] = []
                excel_tables_dict[table_tags[0]].append(table)
                break
    return excel_tables_dict


def compare_variables(table1, table2):
    def _get_vars(table):
        vars = []
        for row in table['body']:
            var = row[0].lower()
            var = re.sub('\(.+\)', '', var).strip()
            vars.append(var)
        return vars

    vars1 = _get_vars(table1)
    vars2 = _get_vars(table2)
    if len(vars1) != len(vars2):
        log_msg(f'number of variables mismatch. html length:{len(vars1)}, excel length: {len(vars2)}')
        missed = list(set(vars2) - set(vars1))
        extra = list(set(vars1) - set(vars2))
        if missed:
            log_msg(f'variables are missing in html table: {missed}')
        if extra:
            log_msg(f'variables are extra in html table: {extra}')
        return
    for i in range(len(vars1)):
        if vars1[i] != vars2[i]:
            log_msg(f'varaible mismatch, html var: {vars1[i]}, excel var: {vars2[i]}')


def compare_data(top3_table_tags, html_tables_dict, excel_tables_dict):
    for top3_tag in top3_table_tags:
        log_msg(f"table: {top3_tag}")
        if len(html_tables_dict.get(top3_tag, [])) < len(excel_tables_dict.get(top3_tag, [])):
            log_msg(f"table is missing from html. class:{top3_tag}")
            continue
        if len(excel_tables_dict.get(top3_tag, [])) == 0:
            log_msg(f"table is missing from excel. class:{top3_tag}")
            continue
        for i, table in enumerate(html_tables_dict[top3_tag]):
            if len(excel_tables_dict[top3_tag]) <= i:
                log_msg('there is no table from excel to compare')
                continue
            compare_variables(table, excel_tables_dict[top3_tag][i])


if __name__ == "__main__":
    with open('report.txt', 'w') as f:
        f.write('')

    path = r"data\current\AFL\000000497723000055\afl-20221231.htm"
    log_msg(f'report for path:{path}')
    html_tables = extract_html_tables(path)
    excel_tables = process_finance_excel(os.path.join(os.path.dirname(path), 
                                                      'Financial_Report.xlsx'))
    with open(os.path.join(os.path.dirname(path), 'xbrl_pre.json')) as f:
        hierarchy_data = json.load(f)
    with open(r"C:\Users\Manohar\Downloads\response.json", "r", encoding="utf-8") as f:
        xbrl_data = json.load(f)
    tags_list = get_table_tags_reported(hierarchy_data, top3_table_tags)
    context_ref = get_current_date_contextref(xbrl_data)
    html_tables_dict = classify_tables_html(html_tables, tags_list, context_ref)
    excel_tables_dict = classify_tables_excel(excel_tables, tags_list)
    compare_data(top3_table_tags, html_tables_dict, excel_tables_dict)
    log_msg('')



# code to test extracted variables in html tables