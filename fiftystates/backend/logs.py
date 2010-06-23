import sys
import logging

from fiftystates.backend import db
from fiftystates.backend.utils import timestamp_to_dt

_initialized = False


class MongoHandler(logging.Handler):
    def __init__(self, collection, level=logging.NOTSET):
        logging.Handler.__init__(self, level)
        self.collection = collection

    def emit(self, record):
        self.collection.save({'created': timestamp_to_dt(record.created),
                              'level_no': record.levelno,
                              'level_name': record.levelname,
                              'process': record.process,
                              'process_name': record.processName,
                              'thread': record.thread,
                              'thread_name': record.threadName,
                              'file_name': record.filename,
                              'path_name': record.pathname,
                              'module': record.module,
                              'func_name': record.funcName,
                              'msecs': record.msecs,
                              'exc_text': record.exc_text or None,
                              'exc_info': record.exc_info or None,
                              'message': record.getMessage(),
                              'argv': sys.argv})


def init_mongo_logging(collection_name='log'):
    global _initialized

    if _initialized:
        return

    if collection_name not in db.collection_names():
        db.create_collection(collection_name, capped=True, size=134217728)

    logging.getLogger().addHandler(MongoHandler(db[collection_name]))
    logging.getLogger().setLevel(logging.NOTSET)
