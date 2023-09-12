from typing import List

from pymongo import MongoClient, UpdateOne

from config import get_logger
from config import MongoDbKLGConfig
from constants.constants import ChainConstant, GraphCreditScoreConfigKeys
from utils.retry_handler import retry_handler
from utils.time_execute_decorator import sync_log_time_exe, TimeExeTag
from utils.dict_utils import flatten_dict, to_string_keys_dict
logger = get_logger('MongoDB')


class MongoDB:
    def __init__(self, graph):
        if not graph:
            graph = MongoDbKLGConfig.HOST

        self.connection_url = graph.split('@')[-1]
        self.connection = MongoClient(graph)
        self.mongo_db = self.connection[MongoDbKLGConfig.KLG_DATABASE]

        self._wallets_col = self.mongo_db['wallets']
        self._multichain_wallets_col = self.mongo_db['multichain_wallets']
        self._multichain_wallets_credit_scores_col = self.mongo_db['multichain_wallets_credit_scores']
        self._profiles_col = self.mongo_db['profiles']

        self._configs_col = self.mongo_db['configs']

        self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        ...

    def get_smart_contract(self, chain_id, address):
        key = f"{chain_id}_{address}"
        filter_ = {"_id": key}
        return self.mongo_db['smart_contracts'].find_one(filter_)

    #######################
    #      Wallets        #
    #######################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallet(self, address, chain_id=None):
        if chain_id is None:
            filter_statement = {'_id': address}
            doc = self._multichain_wallets_col.find_one(filter_statement)
        else:
            filter_statement = {'_id': f'{chain_id}_{address}'}
            doc = self._wallets_col.find_one(filter_statement)
        return doc

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_with_batch_idx(self, batch_idx=1, batch_size=10000, chain_id=None, projection=None):
        projection_statement = self.get_projection_statement(projection)
        if chain_id is None:
            filter_statement = {'flagged': batch_idx}
            cursor = self._multichain_wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        else:
            filter_statement = {'flagged': batch_idx, 'chainId': chain_id}
            cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_selective_wallet_addresses(self, batch_size=10000):
        filter_statement = {'selective': True}
        projection_statement = self.get_projection_statement(['address'])
        cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        data = [doc['address'] for doc in cursor]
        return data

    def check_exists_wallets(self, addresses, batch_size):
        keys = []
        for address in addresses:
            keys.extend([f'{chain_id}_{address}' for chain_id in ChainConstant.all])

        filter_statement = {'_id': {'$in': keys}}
        cursor = self._multichain_wallets_col.find(filter=filter_statement, projection=['chainId', 'address', 'createdAt'], batch_size=batch_size)
        wallets = []
        for doc in cursor:
            wallets.append({
                'chain_id': doc['chainId'],
                'address': doc['address'],
                'created_at': doc.get('createdAt')
            })
        return wallets

    def get_wallet_addresses(self, chain_id=None, batch_size=100000, update_created_at=False):
        try:
            if chain_id:
                filter_statement = {'chainId': chain_id}
                cursor = self._wallets_col.find(filter=filter_statement, projection=['address'], batch_size=batch_size)
            else:
                filter_statement = {}
                if update_created_at:
                    filter_statement = {'updatedCreatedAt': {'$exists': False}}
                cursor = self._multichain_wallets_col.find(filter=filter_statement, projection=['address'], batch_size=batch_size)

            addresses = [doc['address'] for doc in cursor]
            return addresses
        except Exception as ex:
            logger.exception(ex)
            return []

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_by_keys(self, keys, projection=None):
        projection_statement = self.get_projection_statement(projection)
        filter_statement = {'_id': {'$in': keys}}
        cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_wallets(self, wallets: List[dict]):
        for wallet in wallets:
            wallet["_id"] = f'{wallet.get("chainId")}_{wallet.get("address")}'

        bulk_operations = [UpdateOne(
            {"_id": item["_id"], "address": item["address"]},
            {"$set": flatten_dict(item)},
            upsert=True
        ) for item in wallets]
        self._wallets_col.bulk_write(bulk_operations)

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_with_flagged(self, batch_size=50000, chain_id=None, reset=False):
        if chain_id is None:
            if not reset:
                filter_statement = {'flagged': {'$exists': False}}
            else:
                filter_statement = {}
            cursor = self._multichain_wallets_col.find(filter_statement, projection=['address'], batch_size=batch_size)
        else:
            if not reset:
                filter_statement = {'chainId': chain_id, 'flagged': {'$exists': False}}
            else:
                filter_statement = {'chainId': chain_id}
            cursor = self._wallets_col.find(filter_statement, projection=['address'], batch_size=batch_size)

        return cursor

    ########################
    #  Multichain wallets  #
    ########################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_statistics_data(self, keys, batch_size=1000):
        filter_statement = {'_id': {'$in': keys}}
        projection = [
            '_id', 'dailyNumberOfTransactions', 'dailyTransactionAmounts',
            'depositInUSD', 'borrowInUSD', 'balanceInUSD',
            'depositChangeLogs', 'borrowChangeLogs', 'balanceChangeLogs', 'createdAt'
        ]
        cursor = self._multichain_wallets_col.find(filter_statement, projection=projection, batch_size=batch_size)

        data = []
        for doc in cursor:
            doc['key'] = doc.pop('_id')
            balance_in_usd = doc.get('balanceInUSD') or 0
            deposit_in_usd = doc.get('depositInUSD') or 0
            borrow_in_usd = doc.get('borrowInUSD') or 0
            doc['wallet_asset'] = balance_in_usd + deposit_in_usd - borrow_in_usd
            data.append(doc)
        return data

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_elite_wallets(self, projection=None, batch_size=1000):
        filter_statement = {'elite': True}
        projection_statement = self.get_projection_statement(projection)
        cursor = self._multichain_wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    def check_exists(self, addresses, batch_size=1000):
        filter_statement = {'_id': {'$in': addresses}}
        cursor = self._multichain_wallets_col.find(filter=filter_statement, projection=['_id'], batch_size=batch_size)
        wallets = []
        for doc in cursor:
            wallets.append(doc['_id'])
        return wallets

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_multichain_wallets(self, wallets: List[dict]):
        for wallet in wallets:
            wallet["_id"] = wallet.get("address")

        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": flatten_dict(item)}, upsert=True) for item in wallets]
        self._multichain_wallets_col.bulk_write(bulk_operations)

    def get_all_multichain_wallets(self):
        return self._multichain_wallets_col.find({})
    ########################
    #       Profiles       #
    ########################

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_profile(self, profile_id):
        return self._profiles_col.find_one({'_id': profile_id})

    #######################
    #        Score        #
    #######################

    def get_wallet_score(self, address):
        filter_statement = {'_id': address}
        doc = self._multichain_wallets_credit_scores_col.find_one(filter_statement)
        return doc

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallet_scores_change_logs(self, chain_id=None, token_address=None, batch_idx=1, batch_size=50000):
        try:
            if chain_id is not None:
                if token_address is not None:
                    token_key = f'{chain_id}_{token_address}'
                    filter_statement = {f'tokens.{token_key}': {'$gt': 0}}
                    cursor = self._multichain_wallets_col.find(filter_statement, projection=['_id'], batch_size=batch_size)
                    addresses = [doc['_id'] for doc in cursor]

                    filter_statement = {'_id': {'$in': addresses}}
                else:
                    filter_statement = {'chainId': chain_id, 'flagged': batch_idx}
                    cursor = self._wallets_col.find(filter_statement, projection=['address'], batch_size=batch_size)
                    addresses = [doc['address'] for doc in cursor]

                    filter_statement = {'_id': {'$in': addresses}}
            else:
                filter_statement = {'flagged': batch_idx}

            cursor = self._multichain_wallets_credit_scores_col.find(
                filter_statement, projection=['address', 'creditScoreChangeLogs'], batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_multichain_wallets_score_with_batch_idx(self, batch_idx=1, batch_size=10000, projection=None):
        projection_statement = self.get_projection_statement(projection)
        filter_statement = {'flagged': batch_idx}
        cursor = self._multichain_wallets_credit_scores_col.find(filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    @sync_log_time_exe(tag=TimeExeTag.database)
    @retry_handler
    def update_multichain_wallet_scores(self, wallet_scores, _type='update'):
        for wallet in wallet_scores:
            wallet["_id"] = wallet.get("address")

        if _type == 'replace':
            bulk_operations = [UpdateOne(
                {"_id": item["_id"]},
                {"$set": to_string_keys_dict(item)},
                upsert=True
            ) for item in wallet_scores]
        else:
            bulk_operations = [UpdateOne(
                {"_id": item["_id"]},
                {"$set": flatten_dict(item)},
                upsert=True
            ) for item in wallet_scores]
        self._multichain_wallets_credit_scores_col.bulk_write(bulk_operations)

    #######################
    #       Config        #
    #######################

    def get_wallet_statistics(self):
        filter_statement = {'_id': GraphCreditScoreConfigKeys.wallet_statistics}
        projection = [
            'total_asset', 'age_of_account',
            'transaction_amount', 'frequency_of_transaction',
            'deposit', 'borrow', 'frequency_of_dapp_transaction',
            'number_of_dapps'
        ]
        doc = self._configs_col.find_one(filter_statement, projection=projection)
        return doc

    def get_wallet_flagged_state(self, chain_id=None):
        if chain_id is None:
            filter_statement = {'_id': GraphCreditScoreConfigKeys.multichain_wallets_flagged_state}
        else:
            filter_statement = {'_id': GraphCreditScoreConfigKeys.wallets_flagged_state + '_' + chain_id}
        doc = self._configs_col.find_one(filter_statement)
        return doc

    @retry_handler
    def update_configs(self, configs, _type='update'):
        if _type == 'replace':
            bulk_operations = [UpdateOne(
                {"_id": item["id"]},
                {"$set": to_string_keys_dict(item)},
                upsert=True
            ) for item in configs]
        else:
            bulk_operations = [UpdateOne(
                {"_id": item["id"]},
                {"$set": flatten_dict(item)},
                upsert=True
            ) for item in configs]
        self._configs_col.bulk_write(bulk_operations)

    #######################
    #       Common        #
    #######################

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return None

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements
