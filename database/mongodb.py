from pymongo import MongoClient
from pymongo import UpdateOne
from config import MongoConfig
from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler

logger = get_logger('Blockchain ETL')


class MongoDB:
    def __init__(self, connection_url=None, database=None, db_prefix=""):
        self._conn = None
        if not connection_url:
            connection_url = MongoConfig.CONNECTION_URL

        _db = MongoConfig.DATABASE
        if database:
            _db = database

        # self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)
        if db_prefix:
            db_name = db_prefix + "_" + _db
        else:
            db_name = _db

        self.mongo_db = self.connection[db_name]

    def get_document(self, collection, conditions, args=None):
        _collection = self.mongo_db[collection]
        if args:
            result = _collection.find_one(conditions, args)
        else:
            result = _collection.find_one(conditions)
        return result

    def get_documents(self, collection, conditions, args=None):
        _collection = self.mongo_db[collection]
        if args:
            result = _collection.find(conditions, args)
        else:
            result = _collection.find(conditions)
        return result

    @retry_handler
    def update_document(self, collection, document, upsert=True):
        _collection = self.mongo_db[collection]
        try:
            _collection.update({"_id": document["_id"]}, {"$set": flatten_dict(document)}, upsert=upsert)
            success = True
        except Exception as e:
            logger.error(e)
            success = False

        return success

    @retry_handler
    def update_documents(self, collection, documents, upsert=True):
        _collection = self.mongo_db[collection]
        try:
            bulk_operations = [UpdateOne({'_id': document['_id']}, {"$set": flatten_dict(document)}, upsert=upsert)
                               for document in documents]
            _collection.bulk_write(bulk_operations)
            success = True
        except Exception as e:
            logger.error(e)
            success = False

        return success


def flatten_dict(d):
    out = {}
    for key, val in d.items():
        if isinstance(val, dict):
            val = [val]
        if isinstance(val, list):
            for subdict in val:
                deeper = flatten_dict(subdict).items()
                out.update({key + '.' + key2: val2 for key2, val2 in deeper})
        else:
            out[key] = val
    return out
