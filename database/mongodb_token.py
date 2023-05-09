import time

from pymongo import MongoClient

from config import get_logger, ArangoDBConfig

logger = get_logger('MongoDB Token')


class MongoDbToken:
    def __init__(self, graph):
        self.mongo = MongoClient(graph)
        self._db = self.mongo[ArangoDBConfig.TOKEN_DATABASE]

        self._tokens_col = self._db[ArangoDBConfig.TOKENS]

    def get_tokens_credit_score(self):
        start_time = time.time()
        cursor = self._tokens_col.find({}, projection=['_id', 'creditScore', 'price']).batch_size(1000)
        data = list(cursor)

        tokens = {}
        for token in data:
            _id = token['_id']
            tokens[_id] = {'score': token.get('creditScore') or 0, 'price': token.get('price') or 0}

        logger.info(f'Load {len(tokens)} tokens score take {time.time() - start_time} seconds')
        return tokens
