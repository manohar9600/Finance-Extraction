import os
import sys
current_script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_script_dir)
sys.path.append(parent_dir)
from extraction.db_functions import DBFunctions


# adding document id to values table
dbf = DBFunctions()
values_df = dbf.get_table_data('values')
doc_df = dbf.get_table_data('documents')

for i, row in values_df.iterrows():
    _d = doc_df[doc_df['publisheddate'] == row['period']]
    _d = _d[_d['companyid'] == row['companyid']]
    if not _d.empty:
        docid = _d['id'].iloc[0]
        query = f"UPDATE values SET documentid = {docid} WHERE id = {row['id']}"
        dbf.update_query(query)