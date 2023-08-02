import tornado.ioloop
import tornado.web
import os
import json
from glob import glob
from loguru import logger


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
        logger.info("--- intial request received ---")

        data = {
            "companies": [os.path.basename(p) for p in glob("data/*")]
        }

        self.set_header('Content-Type', 'application/json')
        self.write(data)


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
        folder_path = os.path.join('data', data['company'])
        file_path = os.path.join(folder_path, 'metadata.json')
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                metadata = json.load(file)
        else:
            metadata = {
                "name": data['company'],
                "cover": "https://images.unsplash.com/photo-1560179707-f14e90ef3623"
            }

        metadata['files'] = [os.path.basename(p) for p in glob(os.path.join(folder_path, '*')) if os.path.isdir(p)]
        
        self.set_header('Content-Type', 'application/json')
        self.write(metadata)


class FileHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "Content-Type")
        self.set_header('Access-Control-Allow-Methods', 'POST, OPTIONS')

    def options(self):
        # This method handles the pre-flight OPTIONS request.
        # It simply responds with the CORS headers.
        self.set_status(204)
        self.finish()

    def post(self):
        logger.info("--- file request received ---")
        data = json.loads(self.request.body)
        file_path = os.path.join('data', data['company'], data['file'], 'tables.json')
        with open(file_path, 'r') as file:
            tables = json.load(file)['tables']

        f_tables = []
        for table in tables:
            if table['class']:
                if 'tableHTML' in table:
                    table.pop('tableHTML')
                f_tables.append(table)

        self.set_header('Content-Type', 'application/json')
        self.write({"tables": f_tables})


def make_app():
    return tornado.web.Application([
        (r"/all", AllHandler),
        (r"/meta", Metadata),
        (r"/file", FileHandler),
    ])


if __name__ == "__main__":
    app = make_app()
    port = 8888
    app.listen(port)
    logger.info(f"listening on port {port}")
    tornado.ioloop.IOLoop.current().start()
