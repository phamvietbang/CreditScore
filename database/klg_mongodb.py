from pymongo import MongoClient

from config import MongoDbKLGConfig


class MongoDbKLG:
    def __init__(self, connection_url, chain_id='0x38', chain_name='BSC',
                 update_after_updated_time=True):
        self.update_after_updated_time = update_after_updated_time
        self.chain_name = chain_name
        self.chain_id = chain_id
        if not connection_url:
            connection_url = MongoDbKLGConfig.HOST

        self.connection_url = connection_url.split('@')[-1]
        self.connection = MongoClient(connection_url)
        self.mongo_db = self.connection[MongoDbKLGConfig.KLG_DATABASE]

        self._wallets_col = self.mongo_db[MongoDbKLGConfig.WALLETS]
        self._multichain_wallets_col = self.mongo_db[MongoDbKLGConfig.MULTICHAIN_WALLETS]
        self._deposits_col = self.mongo_db[MongoDbKLGConfig.DEPOSITS]
        self._withdraws_col = self.mongo_db[MongoDbKLGConfig.WITHDRAWS]
        self._repays_col = self.mongo_db[MongoDbKLGConfig.REPAYS]
        self._borrows_col = self.mongo_db[MongoDbKLGConfig.BORROWS]
        self._liquidates_col = self.mongo_db[MongoDbKLGConfig.LIQUIDATES]
        self._smart_contracts_col = self.mongo_db[MongoDbKLGConfig.SMART_CONTRACTS]

    def get_smart_contracts(self, ids):
        if not ids:
            return list()
        filter_statement = {
            "_id": {"$in": ids}
        }
        cursor = self._smart_contracts_col.find(filter_statement, batch_size=10000)
        smart_contracts = []
        for smart_contract in cursor:
            smart_contracts.append(smart_contract)

        return smart_contracts

    def get_smart_contract(self, id_):
        if not id_:
            return list()
        filter_statement = {
            "_id": id_
        }
        smart_contract = self._smart_contracts_col.find_one(filter_statement)
        return smart_contract

    def get_ctoken_information(self, id_):
        filter_statement = {
            "_id": id_
        }
        smart_contract = self._smart_contracts_col.find_one(filter_statement)
        if smart_contract:
            return smart_contract["lendingInfo"]
        return None