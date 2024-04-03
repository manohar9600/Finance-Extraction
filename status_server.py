from server import MainHandler
import tornado
from loguru import logger
from extraction.db_functions import RedisFunctions
import json

rd_fns = RedisFunctions()


class StatusHandler(MainHandler):
    def post(self):
        logger.info("--- status request received ---")
        data = json.loads(self.request.body)
        status_data = rd_fns.get_data(data['uid'])
        self.set_header('Content-Type', 'application/json')
        self.write(status_data)


def make_app():
    return tornado.web.Application([
        (r"/", StatusHandler),
    ])


if __name__ == "__main__":
    app = make_app()
    port = 9013
    app.listen(port)
    logger.info(f"listening on port {port}")
    tornado.ioloop.IOLoop.current().start()