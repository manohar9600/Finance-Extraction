import os
import tornado.ioloop
import tornado.web
import json
import pandas as pd
import psycopg2
from extraction.db_functions import MinioDBFunctions, get_segment_data
from extraction.data_processor import extract_segment_information
from loguru import logger
from extraction.db_functions import DBFunctions
from extraction.utils import get_latest_file
from company_scrapper.companyinfo import get_ticker_data
from extraction.gpt_functions import *
from agents.gpt import GPT

master_folder = 'data/Current'


class MainHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header("Access-Control-Allow-Headers", "Content-Type")
        self.set_header('Access-Control-Allow-Methods', 'GET, OPTIONS')

    def options(self):
        # This method handles the pre-flight OPTIONS request.
        # It simply responds with the CORS headers.
        self.set_status(204)
        self.finish()


class AllHandler(MainHandler):
    def get(self):
        logger.info("--- request received for companies list ---")
        conn = psycopg2.connect(database="ProdDB",
                                    host="localhost",
                                    user="postgres",
                                    password="manu1234",
                                    port="5432")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM companies")
        columns = [desc[0] for desc in cursor.description][1:]
        companies_data = [d[1:] for d in cursor.fetchall()]
        response = {
            'columns': columns,
            'companies': companies_data
        }

        self.set_header('Content-Type', 'application/json')
        self.write(response)


def get_company_data(company_symbol):
    conn = psycopg2.connect(database="ProdDB",
                                host="localhost",
                                user="postgres",
                                password="manu1234",
                                port="5432")
    cursor = conn.cursor()
    cursor.execute(f"SELECT id FROM companies where \"Symbol\"='{company_symbol}'")
    comp_id = cursor.fetchone()[0]
    
    query = f"WITH FilteredOrders AS (SELECT *, type as valuetye FROM values WHERE values.companyid = {comp_id})\
        SELECT * FROM FilteredOrders AS fo RIGHT JOIN variables ON fo.variableid = variables.id\
            LEFT JOIN documents ON fo.documentid = documents.id ORDER by variables.id"
    cursor.execute(query)
    df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
    conn.close()
    return df


def get_cleaned_row(row_dict):
    minio_server_url = 'http://localhost:9000/secreports/'
    required_fields = ["value", "type"]
    final_dict = {}
    for key in row_dict:
        if key in required_fields:
            final_dict[key] = row_dict[key]
    files = MinioDBFunctions('secreports').list_objects(
        row_dict['folderlocation'].replace("\\", "/"))
    # temporary solution
    target_file = ''
    for file in files:
        if 'index' in file.split('/')[-1]:
            continue
        if '.htm' in file.split('/')[-1] or '.html' in file.split('/')[-1]:
            target_file = minio_server_url + file
            break
    final_dict['filelocation'] = target_file
    return final_dict


class CompanyData(MainHandler): 
    
    def post(self):
        logger.info("--- company data request received ---")
        data = json.loads(self.request.body)
        logger.info(f"got request for data of company:{data['company']}")
        finance_tables = self.get_finance_tables(data)
        seg_tables = get_segment_data(data['company'])
        comp_info = DBFunctions().get_company_info(data['company'])
        products = self.get_products_info(data['company'])
        other_info = get_ticker_data(data['company'])
        company_overview = {"products": products, "otherinfo": other_info}

        # debugging code
        debug = {}
        debug['latestfile'] = get_latest_file("data/Current/" + data['company']).replace("data/Current/", "http://192.168.1.4:9000/secreports/")

        self.set_header('Content-Type', 'application/json')
        self.write({'info': comp_info, 'financeTables': finance_tables, 
                    'segmentTables': seg_tables, 'products': products, 'companyOverview': company_overview, 'debug': debug})

    def get_finance_tables(self, data):
        company_data = get_company_data(data['company'])
        columns = company_data
        df = get_company_data(data['company'])
        columns = sorted([d for d in df.period.unique() if d is not None], reverse=True)
        tables = []
        for table_name, table_grp in df.groupby('table'):
            table = []
            for _, grp in table_grp.groupby('variable', sort=False):
                table_row = ["" for _ in range(len(columns)+1)]
                table_row[0] = {'value': grp.iloc[0]['variable']}
                for _, row in grp.iterrows():
                    if row['period'] is None:
                        continue
                    index = columns.index(row['period'])
                    table_row[index+1] = get_cleaned_row(row.to_dict())
                table.append(table_row)

            table = {
                "class": table_name,
                "header": [""] + [d.strftime("%b %Y") for d in columns],
                "body": table
            }
            tables.append(table)
        final_output = {'income statement': {}, 'balance sheet': {}, 'cash flow': {}}
        for table in tables:
            if not table['class'] in final_output:
                continue
            final_output[table['class']] = table

        return final_output

    def get_products_info(self, symbol):
        file_path = os.path.join(master_folder, symbol, "segments_info.json")
        data = {}
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
        return data


class Metadata(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header("Access-Control-Allow-Headers", "Content-Type")
        self.set_header('Access-Control-Allow-Methods', 'POST, OPTIONS')

    def options(self):
        # This method handles the pre-flight OPTIONS request.
        # It simply responds with the CORS headers.
        self.set_status(204)
        self.finish()
    
    def post(self):
        logger.info("--- company meta request received ---")
        data = json.loads(self.request.body)        
        metadata = {
            "name": data['company'],
            "cover": "https://images.unsplash.com/photo-1560179707-f14e90ef3623"
        }        
        self.set_header('Content-Type', 'application/json')
        self.write(metadata)


class CompanyOverview(tornado.web.RequestHandler):
    
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header("Access-Control-Allow-Headers", "Content-Type")
        self.set_header('Access-Control-Allow-Methods', 'POST, OPTIONS')

    def options(self):
        # This method handles the pre-flight OPTIONS request.
        # It simply responds with the CORS headers.
        self.set_status(204)
        self.finish()
        
    def post(self):
        logger.info("--- company data request received ---")
        data = json.loads(self.request.body)
        ticker_data = get_ticker_data(data['company'])
        self.set_header('Content-Type', 'application/json')
        self.write(ticker_data)
        

class GPTHandler(MainHandler):
    def post(self):
        logger.info("--- question received ---")
        data = json.loads(self.request.body)
        self.set_header('Content-Type', 'application/json')
        self.set_header('Cache-Control', 'no-cache')
        gpt = GPT(data['uid'])
        answer = gpt.process_question(data['question'])
        for s in answer:
            self.write(json.dumps(s))
            self.write('<sep>')
            self.flush()
        # self.finish()
        # self.write(answer)


def make_app():
    return tornado.web.Application([
        (r"/all", AllHandler),
        (r"/meta", Metadata),
        (r"/companydata", CompanyData),
        (r"/companyoverview", CompanyOverview),
        (r"/gpt", GPTHandler),
    ])


if __name__ == "__main__":
    app = make_app()
    port = 9110
    app.listen(port)
    logger.info(f"listening on port {port}")
    tornado.ioloop.IOLoop.current().start()
