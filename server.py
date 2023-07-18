import tornado.ioloop
import tornado.web
import json
from glob import glob
from loguru import logger


class Metadata(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET, OPTIONS')

    def options(self):
        # This method handles the pre-flight OPTIONS request.
        # It simply responds with the CORS headers.
        self.set_status(204)
        self.finish()

    def get(self):
        logger.info("--- metadata request received ---")
        with open(r'data\aapl\metadata.json', 'r') as file:
            metadata = json.load(file)

        metadata['files'] = list(glob(r"data\aapl\*\tables.json"))

        self.set_header('Content-Type', 'application/json')
        self.write({"tables": metadata})


class FileHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'GET, OPTIONS')

    def options(self):
        # This method handles the pre-flight OPTIONS request.
        # It simply responds with the CORS headers.
        self.set_status(204)
        self.finish()

    def get(self):
        logger.info("--- file request received ---")
        data = json.loads(self.request.body)
        with open(data['file'], 'r') as file:
            tables = json.load(file)['tables']

        f_tables = []
        for table in tables:
            if table['class']:
                table.pop('tableHTML')
                f_tables.append(table)

        self.set_header('Content-Type', 'application/json')
        self.write({"tables": f_tables})


def make_app():
    return tornado.web.Application([
        (r"/meta", Metadata),
        (r"/file", FileHandler),
    ])


if __name__ == "__main__":
    app = make_app()
    port = 8888
    app.listen(port)
    logger.info(f"listening on port {port}")
    tornado.ioloop.IOLoop.current().start()
