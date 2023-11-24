import tornado.ioloop
import tornado.web
import os
import json
from glob import glob
import pandas as pd
import psycopg2
from loguru import logger


master_folder = 'data/current'
class AllHandler(tornado.web.RequestHandler):
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
    
    query = f"WITH FilteredOrders AS (SELECT * FROM values WHERE values.companyid = {comp_id}) SELECT * FROM FilteredOrders AS fo RIGHT JOIN variables ON fo.variableid = variables.id ORDER by variables.id"
    cursor.execute(query)
    df = pd.DataFrame(cursor, columns=[desc[0] for desc in cursor.description])
    conn.close()
    return df


class CompanyData(tornado.web.RequestHandler):
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
        logger.info(f"got request for data of company:{data['company']}")
        company_data = get_company_data(data['company'])
        columns = company_data
        df = get_company_data(data['company'])
        columns = sorted([d for d in df.period.unique() if d is not None], reverse=True)
        tables = []
        for table_name, table_grp in df.groupby('table'):
            table = []
            for _, grp in table_grp.groupby('variable', sort=False):
                table_row = ["" for _ in range(len(columns)+1)]
                table_row[0] = grp.iloc[0]['variable']
                for _, row in grp.iterrows():
                    if row['period'] is None:
                        continue
                    index = columns.index(row['period'])
                    table_row[index+1] = row['value']
                table.append(table_row)

            table = {
                "class": table_name,
                "header": [""] + [d.strftime("%d-%m-%Y") for d in columns],
                "body": table
            }
            tables.append(table)
        pref_dict = {'income statement': 1, 'balance sheet': 2, 'cash flow': 3}
        tables = sorted(tables, key=lambda x:pref_dict[x['class']])
        self.set_header('Content-Type', 'application/json')
        self.write({'tables': tables})


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


def make_app():
    return tornado.web.Application([
        (r"/all", AllHandler),
        (r"/meta", Metadata),
        (r"/companydata", CompanyData),
    ])


if __name__ == "__main__":
    app = make_app()
    port = 8888
    app.listen(port)
    logger.info(f"listening on port {port}")
    tornado.ioloop.IOLoop.current().start()
