import sys

import pymongo
from pymongo import MongoClient, UpdateOne

from config import MongoConfig
from constants.mongo_constants import MongoDBCollections
from utils.dict_utils import flatten_dict, delete_none
from utils.logger_utils import get_logger
from utils.retry_handler import retry_handler
from utils.time_execute_decorator import sync_log_time_exe, TimeExeTag

logger = get_logger('MongoDBKLG')


class MongoDBKLG:
    def __init__(self, connection_url=None, database="knowledge_graph"):
        if not connection_url:
            connection_url = MongoConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

        self._wallets_col = self.mongo_db[MongoDBCollections.wallets]
        self._multichain_wallets_col = self.mongo_db[MongoDBCollections.multichain_wallets]
        self._multichain_wallets_credit_score_col = self.mongo_db[MongoDBCollections.multichain_wallets_credit_scores]
        self._projects_col = self.mongo_db[MongoDBCollections.projects]
        self._smart_contracts_col = self.mongo_db[MongoDBCollections.smart_contracts]
        self._relationships_col = self.mongo_db[MongoDBCollections.relationships]
        self._call_smart_contracts_col = self.mongo_db[MongoDBCollections.call_smart_contracts]

        self._abi_col = self.mongo_db[MongoDBCollections.abi]
        self._configs_col = self.mongo_db[MongoDBCollections.configs]
        self._is_part_ofs_col = self.mongo_db[MongoDBCollections.is_part_ofs]

        # self._create_index()

    #######################
    #       Index         #
    #######################

    def _create_index(self):
        # Wallet index
        wallets_col_indexes = self._wallets_col.index_information()
        if 'wallets_flagged_chainId_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('flagged', pymongo.ASCENDING), ('chainId', pymongo.ASCENDING)],
                name='wallets_flagged_chainId_index', background=True
            )
        if 'wallets_tags_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('tags', pymongo.ASCENDING)],
                name='wallets_tags_index', background=True, sparse=True
            )
        if 'wallets_newElite_chainId_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('newElite', pymongo.ASCENDING), ('chainId', pymongo.ASCENDING)],
                name='wallets_newElite_chainId_index', background=True, sparse=True
            )
        if 'wallets_newTarget_chainId_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('newTarget', pymongo.ASCENDING), ('chainId', pymongo.ASCENDING)],
                name='wallets_newTarget_chainId_index', background=True, sparse=True
            )
        if 'wallets_elite_chainId_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('elite', pymongo.ASCENDING), ('chainId', pymongo.ASCENDING)],
                name='wallets_elite_chainId_index', background=True, sparse=True
            )
        if 'wallets_selective_index' not in wallets_col_indexes:
            self._wallets_col.create_index(
                [('selective', pymongo.ASCENDING)],
                name='wallets_selective_index', background=True, sparse=True
            )

        # Multichain wallet index
        multichain_wallets_col_indexes = self._multichain_wallets_col.index_information()
        if 'multichain_wallets_flagged_index' not in multichain_wallets_col_indexes:
            self._multichain_wallets_col.create_index(
                [('flagged', pymongo.ASCENDING)],
                name='multichain_wallets_flagged_index', background=True
            )

        # Project index
        projects_col_indexes = self._projects_col.index_information()
        if 'projects_sources_index' not in projects_col_indexes:
            self._projects_col.create_index(
                [('sources', pymongo.ASCENDING)],
                name='projects_sources_index', background=True
            )
        if 'projects_deployedChains_index' not in projects_col_indexes:
            self._projects_col.create_index(
                [('deployedChains', pymongo.ASCENDING)],
                name='projects_deployedChains_index', background=True
            )

        # Contract index
        contracts_col_indexes = self._smart_contracts_col.index_information()
        if 'smart_contracts_tags_index' not in contracts_col_indexes:
            self._smart_contracts_col.create_index(
                [('tags', pymongo.ASCENDING)],
                name='smart_contracts_tags_index', background=True
            )
        if 'smart_contracts_idCoingecko_index' not in contracts_col_indexes:
            self._smart_contracts_col.create_index(
                [('idCoingecko', pymongo.ASCENDING)],
                name='smart_contracts_idCoingecko_index', background=True, sparse=True
            )

    #######################
    #      Project        #
    #######################
    @retry_handler
    def update_projects(self, data: list):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": flatten_dict(item)}, upsert=True) for item in data]
        self._projects_col.bulk_write(bulk_operations)

    def get_all_projects_key(self):
        try:
            cursor = self._projects_col.find(filter={}, projection={"tvlChangeLogs": True}, batch_size=10000)
            data = {}
            for doc in cursor:
                data[doc['_id']] = doc.get('tvlChangeLogs')
            return data
        except Exception as ex:
            logger.exception(ex)
        return {}

    def get_project_by_types(self, type_=None, category=None, projection=None):
        filter_statement = {
            "sources": type_
        }
        if category is not None:
            filter_statement['category'] = category

        projection_statement = self.get_projection_statement(projection)
        try:
            cursor = self._projects_col.find(filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_projects(self, projection=None):
        projection_statement = self.get_projection_statement(projection)

        try:
            cursor = self._projects_col.find(filter={}, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_projects_by_keys(self, keys, projection=None):
        filter_statement = {"_id": {"$in": keys}}
        projection_statement = self.get_projection_statement(projection)
        try:
            cursor = self._projects_col.find(filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    #######################
    #      Contract       #
    #######################
    @retry_handler
    def update_contracts(self, data):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": flatten_dict(item)}, upsert=True) for item in data]
        self._smart_contracts_col.bulk_write(bulk_operations)

    def get_contracts_without_created_at(self):  # return cursor of obj not string
        try:
            filter_statement = {
                "createdAt": {"$exists": False}
            }
            cursor = self._smart_contracts_col.find(filter_statement, {'_id': 1}, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_contracts(self, tags: list = None, chain_id: str = None, keys: list = None, projection=None):
        try:
            filter_statement = {}
            if tags:
                filter_statement['tags'] = {'$in': tags}

            if keys:
                filter_statement['_id'] = {'$in': keys}

            if chain_id:
                filter_statement['chainId'] = chain_id
            if projection:
                projection_statement = self.get_projection_statement(projection)
                cursor = self._smart_contracts_col.find(
                    filter=filter_statement, projection=projection_statement, batch_size=1000)
            else:
                cursor = self._smart_contracts_col.find(
                    filter=filter_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_all_contract_without_check_tag(self, chain_id, limit=1000):
        filter_statement = {
            "chainId": chain_id,
            "checkTag": {"$exists": False}
        }
        projection_statement = {
            "address": 1,
            "chainId": 1,
            "tags": 1,
            "_id": 0
        }
        cursor = self._smart_contracts_col.find(
            filter=filter_statement, projection=projection_statement, batch_size=1000).limit(limit)
        return cursor

    def get_new_contracts(self, chain_id: str = None):  # return cursor of obj not string
        try:
            filter_statement = {
                "isNew": True,
            }
            if chain_id is not None:
                filter_statement["chainId"] = chain_id
            projection_statement = {
                "address": 1,
                "_id": 0
            }
            cursor = self._smart_contracts_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_top_contracts_called(self, limit: int = 100):
        try:
            filter_statement = {
                "lastMonthCalls": {"$exists": True},
            }
            cursor = self._smart_contracts_col.find(
                filter=filter_statement, batch_size=1000).sort("lastMonthCalls", pymongo.DESCENDING).limit(limit)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @retry_handler
    def update_call_contracts(self, call_contracts):
        bulk_operations = [UpdateOne({"_id": call_contract["_id"]}, {"$set": flatten_dict(call_contract)}, upsert=True)
                           for call_contract in call_contracts]
        self._call_smart_contracts_col.bulk_write(bulk_operations)

    #######################
    #       Token         #
    #######################
    def get_tokens(self, projection=None):
        try:
            projection_statement = self.get_projection_statement(projection)
            filter_statement = {
                'idCoingecko': {"$exists": True}
            }
            cursor = self._smart_contracts_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_price(self, token, chain_id):
        key = f"{chain_id}_{token}"
        try:
            filter_statement = {
                "_id": key
            }
            projection_statement = {"price": 1}
            cursor = self._smart_contracts_col.find_one(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
            if cursor:
                return cursor["price"]
            else:
                return 0
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_top_tokens(self, chain_id: str, limit=200):
        try:
            filter_statement = {
                "idCoingecko": {"$exists": True},
                "chainId": chain_id
            }
            cursor = self._smart_contracts_col.find(
                filter=filter_statement, batch_size=1000).sort("marketCap", pymongo.DESCENDING).limit(limit)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_top_token_symbol_mapping(self, chain_id, limit=500):
        try:
            filter_statement = {
                "idCoingecko": {"$ne": None},
                "chainId": chain_id
            }
            projection_statement = {
                'symbol': 1
            }
            cursor = self._smart_contracts_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000).sort(
                "marketCap", pymongo.DESCENDING).limit(limit)

            tokens = {}
            for doc in cursor:
                key = doc['_id']
                symbol = doc['symbol']
                if symbol not in tokens:
                    tokens[symbol] = key
            return tokens
        except Exception as ex:
            logger.exception(ex)
        return {}

    def get_selected_token_addresses(self, chain_id: str):  # return cursor of obj not str
        """Get addresses of specific tokens, from collection configs"""
        try:
            filter_statement = {
                "_id": f'top_tokens_{chain_id}'
            }
            top_tokens = self._configs_col.find_one(filter=filter_statement)
            if not top_tokens:
                return []

            addresses = []
            for token in top_tokens['tokens']:
                addresses.append(token['address'])
            return addresses
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_tokens_with_top_holders(self, chain_id, projection=None):
        projection_statement = self.get_projection_statement(projection)
        try:
            filter_statement = {
                "chainId": chain_id,
                "topHolders": {"$ne": None}
            }
            cursor = self._smart_contracts_col.find(filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_top_target_wallet_address(self, chain_id, flag='elite'):  # return cursor of obj not str
        try:
            filter_statement = {
                "chainId": chain_id,
                flag: True
            }
            projection_statement = {
                "address": 1,
                "_id": 0
            }
            cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return []

    def get_prices_by_keys(self, token_keys: list):  # change key to _id
        try:
            filter_statement = {
                "_id": {"$in": token_keys}
            }
            projection_statement = {
                'address': 1,
                'price': 1
            }
            cursor = self._smart_contracts_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_tokens_by_coin_ids(self, coin_ids):
        filter_statement = {
            "idCoingecko": {"$in": coin_ids}
        }
        projection_statement = {
            "address": 1,
            "chainId": 1,
            "idCoingecko": 1,
            "priceChangeLogs": 1,
            "marketCapChangeLogs": 1,
            "tradingVolumeChangeLogs": 1
        }
        tokens = {}
        try:
            cursor = self._smart_contracts_col.find(filter_statement, projection_statement)
            for doc in cursor:
                coin_id = doc['idCoingecko']
                if coin_id not in tokens:
                    tokens[coin_id] = []

                tokens[coin_id].append(doc)
        except Exception as ex:
            logger.exception(ex)
        return tokens

    #######################
    #       Wallet        #
    #######################

    def get_elite_wallets(self, chain_id: str):
        try:
            filter_statement = {
                'elite': True,
                'chainId': chain_id
            }
            projection_statement = {
                'address': 1,
                'chainId': 1
            }
            cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets(self, chain_id=None, batch_idx=None, projection=None, batch_size=50000):
        filter_statement = {}
        if chain_id:
            filter_statement['chainId'] = chain_id
        if batch_idx:
            filter_statement['flagged'] = batch_idx

        projection_statement = self.get_projection_statement(projection)
        cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    def get_wallets_by_keys(self, keys, projection=None):
        projection_statement = self.get_projection_statement(projection)
        try:
            filter_statement = {
                "_id": {"$in": keys}
            }
            cursor = self._wallets_col.find(filter=filter_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_by_addresses(self, addresses, projection=None):
        projection_statement = self.get_projection_statement(projection)
        try:
            filter_statement = {
                "addresses": {"$in": addresses}
            }
            cursor = self._wallets_col.find(filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_addresses(self, label: str, chain_id: str = '0x38',
                              batch_size: int = 1000):  # return cursor of obj not str
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id
            }
            projection_statement = {
                'address': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_addresses_with_flags(
            self, label: str, chain_id: str = '0x38', batch_size: int = 1000, flag: int = 1):  # return cursor of obj not str
        """
        Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id,
                'flagged': flag
            }
            projection_statement = {
                'address': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_fix_wallets_addresses_with_limit(self, label: str, chain_id: str = '0x38',
                                             limit: int = 1000):  # return cursor of obj not str
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                'chainId': chain_id,
                label: False
            }
            projection_statement = {
                'address': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement).limit(limit)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_balance_change_log_wallets_addresses(self, label: str, chain_id: str = '0x38', flag: int = 1):
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id,
                'flagged': flag
            }
            projection_statement = {
                'address': 1,
                'balanceChangeLogs': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @sync_log_time_exe(tag=TimeExeTag.database)
    def get_wallets_addresses_with_limit(self, label: str, chain_id: str = '0x38', limit: int = 1000):
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id
            }
            projection_statement = {
                'address': 1
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement).limit(limit)

            return [item["address"] for item in cursor]
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_by_list_keys(self, keys: list):
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                "_id": {"$in": keys}
            }
            cursor = self._wallets_col.find(filter=filter_statement, batch_size=1000)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_token_balance(self, wallet_addresses: list, chain_id: str = '0x38'):
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            list_keys = [f"{chain_id}_{wallet_address}" for wallet_address in wallet_addresses]
            filter_statement = {
                "_id": {"$in": list_keys}
            }
            projection_statement = {
                'address': 1,
                "createdAt": 1,
                "tokenChangeLogs": 1,
                "_id": 0
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_addresses_with_timestamp(self, label: str, chain_id: str = '0x38', timestamp: int = 0):
        """Get addresses with timestamp in balanceChangeLogs
        """
        try:
            filter_statement = {
                label: True,
                'chainId': chain_id,
                f'balanceChangeLogs.{timestamp}': {"$exists": True}
            }
            projection_statement = {
                'address': 1,
            }
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement)
            return [item["address"] for item in cursor]
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallet_with_filter(self, filter_):
        return self._wallets_col.find(filter_)

    @retry_handler
    def update_wallets(self, data):
        try:
            bulk_operations = [UpdateOne(
                {"_id": item["_id"], 'address': item['address']},
                {"$set": flatten_dict(item)},
                upsert=True
            ) for item in data]
            self._wallets_col.bulk_write(bulk_operations)
        except Exception as e:
            logger.error(f"Err: {e}")
            logger.info("Export each feature!")
            for item in data:
                bulk_operations = []
                flatten_wallet = flatten_dict(item)
                for key in item:
                    flatten_data = {flatten_key: value for flatten_key, value in flatten_wallet.items()
                                    if key in flatten_key}
                    if not flatten_data:
                        continue
                    bulk_operations.append(
                        UpdateOne({"_id": item["_id"], "address": item["address"]},
                                  {"$set": flatten_data}, upsert=True)
                    )
                self._wallets_col.bulk_write(bulk_operations)

    @retry_handler
    def update_wallets_without_flatten(self, data):
        bulk_operations = [UpdateOne(
            {"_id": item["_id"], 'address': item['address']},
            {"$set": item},
            upsert=True
        ) for item in data]
        self._wallets_col.bulk_write(bulk_operations)

    #######################
    #  Multichain wallet  #
    #######################
    def get_multichain_wallets_credit_score(self, addresses):
        try:
            filter_statement = {
                "_id": {"$in": addresses}
            }
            cursor = self._multichain_wallets_credit_score_col.find(filter_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_multichain_wallets(self, addresses):
        try:
            filter_statement = {
                "_id": {"$in": addresses}
            }
            cursor = self._multichain_wallets_col.find(filter_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_multichain_wallets_with_flags(self, flag):
        try:
            filter_statement = {
                "flagged": flag
            }
            cursor = self._multichain_wallets_col.find(filter_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_wallets_with_batch_idx(self, batch_idx=1, batch_size=10000, chain_id=None, projection=None):
        projection_statement = self.get_projection_statement(projection)
        filter_statement = {"flagged": batch_idx}
        if chain_id:
            filter_statement["chainId"] = chain_id
            cursor = self._wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=batch_size)
            return cursor
        cursor = self._multichain_wallets_col.find(
            filter=filter_statement, projection=projection_statement, batch_size=batch_size
        )
        return cursor

    def get_multichain_wallets_created_at(self, addresses):
        try:
            filter_statement = {
                "_id": {"$in": addresses}
            }
            projection_statement = {
                "createdAt": 1
            }
            cursor = self._multichain_wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_multichain_wallets_balance(self, wallet_addresses: list):
        """Get addresses with specific label (elite, target, new_elite, new_target)
        """
        try:
            filter_statement = {
                "_id": {"$in": wallet_addresses}
            }
            projection_statement = {
                "address": 1,
                "createdAt": 1,
                "balanceChangeLogs": 1,
                "tokenChangeLogs": 1,
                "dailyNumberOfTransactionsInEachChain": 1,
                "dailyTransactionAmountsInEachChain": 1,
            }
            cursor = self._multichain_wallets_col.find(
                filter=filter_statement, projection=projection_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    @retry_handler
    def update_multichain_wallets(self, data):
        try:
            bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": flatten_dict(item)}, upsert=True)
                               for item in data]
            self._multichain_wallets_col.bulk_write(bulk_operations)
        except Exception as e:
            logger.error(f"Err: {e}")
            logger.info("Export each feature!")
            for item in data:
                bulk_operations = []
                flatten_wallet = flatten_dict(item)
                for key in item:
                    flatten_data = {flatten_key: value for flatten_key, value in flatten_wallet.items()
                                    if key in flatten_key}
                    if not flatten_data:
                        continue
                    bulk_operations.append(
                        UpdateOne({"_id": item["_id"], "address": item["address"]},
                                  {"$set": flatten_data}, upsert=True)
                    )
                self._multichain_wallets_col.bulk_write(bulk_operations)

    #######################
    #    Relationship     #
    #######################
    @retry_handler
    def update_relationships(self, relationships):
        bulk_operations = [UpdateOne({"_id": relationship["_id"]}, {"$set": flatten_dict(relationship)}, upsert=True)
                           for relationship in relationships]
        self._relationships_col.bulk_write(bulk_operations)

    @retry_handler
    def update_is_part_ofs(self, data):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": flatten_dict(item)}, upsert=True)
                           for item in data]
        self._is_part_ofs_col.bulk_write(bulk_operations)

    #######################
    #        ABI          #
    #######################
    def get_abi(self, abi_names: list):
        try:
            filter_statement = {
                "_id": {"$in": abi_names}
            }
            cursor = self._abi_col.find(filter_statement, batch_size=1000)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return None

    def update_abi(self, data):
        bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": flatten_dict(item)}, upsert=True)
                           for item in data]
        self._abi_col.bulk_write(bulk_operations)

    ######################
    #      Configs       #
    ######################

    def get_config(self, key):
        try:
            config = self._configs_col.find_one({'_id': key})
            return config
        except Exception as ex:
            logger.exception(ex)
        return {}

    def update_config(self, config, merge=True):
        try:
            if merge:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": flatten_dict(config)}, upsert=True)]
            else:
                bulk_operations = [UpdateOne({"_id": config["_id"]}, {"$set": config}, upsert=True)]
            self._configs_col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    def get_wallet_flagged_state(self, chain_id=None):
        if chain_id is None:
            key = 'multichain_wallets_flagged_state'
        else:
            key = f'wallets_flagged_state_{chain_id}'
        filter_statement = {
            "_id": key
        }
        config = self._configs_col.find_one(filter_statement)
        if not config:
            return None
        return config

    def get_new_wallet_by_flags_config(self, chain_id):
        filter_statement = {
            "_id": f'wallet_flags_{chain_id}'
        }
        projection_statement = {
            "newElite": 1,
            "newTarget": 1,
            "_id": 0
        }
        doc = self._configs_col.find_one(filter_statement, projection_statement)
        if not doc:
            return None
        return doc

    def get_new_flag_wallet_in_flags_config(self, chain_id, flag):
        filter_statement = {
            "_id": f'wallet_flags_{chain_id}'
        }
        projection_statement = {
            flag: 1,
            "_id": 0
        }
        doc = self._configs_col.find_one(filter_statement, projection_statement)
        if not doc:
            return None
        return doc

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return {}

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements

    #######################
    #       Common        #
    #######################
    def get_docs(self, collection, keys: list = None, filter_: dict = None, batch_size=1000,
                 projection=None):  # change filter_ to obj
        projection_statement = self.get_projection_statement(projection)

        filter_statement = {}
        if keys:
            filter_statement["_id"] = {"$in": keys}
        if filter_ is not None:
            filter_statement.update(filter_)

        cursor = self.mongo_db[collection].find(
            filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    def update_docs(self, collection_name, data, keep_none=False, merge=True, shard_key=None):
        """If merge is set to True => sub-dictionaries are merged instead of overwritten"""
        try:
            col = self.mongo_db[collection_name]
            # col.insert_many(data, overwrite=True, overwrite_mode='update', keep_none=keep_none, merge=merge)
            bulk_operations = []
            for document in data:
                unset, set_, add_to_set = self.create_update_doc(document, keep_none, merge)
                if not shard_key:
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$unset": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in unset]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$set": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in set_]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$addToSet": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in add_to_set]
                if shard_key:
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$unset": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in unset]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$set": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in set_]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$addToSet": {key: value for key, value in item.items() if key != "_id"}},
                                  upsert=True)
                        for item in add_to_set]
            col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    def remove_out_date_docs(self, collection_name, timestamp, filter_: dict = None):  # change filter to dict
        filter_statement = {
            "lastUpdatedAt": {"$lt": timestamp}
        }
        if filter_ is not None:
            filter_statement.update(filter_)

        self.mongo_db[collection_name].delete_many(filter_statement)

    def remove_docs_by_keys(self, collection_name, keys):
        filter_statement = {
            "_id": {"$in": keys}
        }
        self.mongo_db[collection_name].delete_many(filter_statement)

    @staticmethod
    def create_update_doc(document, keep_none=False, merge=True):
        unset, set_, add_to_set = [], [], []
        if not keep_none:
            doc = flatten_dict(document)
            for key, value in doc.items():
                if value is None:
                    unset.append({
                        "_id": document["_id"],
                        key: ""
                    })
                    continue
                if not merge:
                    continue
                if isinstance(value, list):
                    add_to_set.append(
                        {
                            "_id": document["_id"],
                            key: {"$each": [i for i in value if i]}
                        }
                    )
                else:
                    set_.append(
                        {
                            "_id": document["_id"],
                            key: value
                        }
                    )
        if not merge:
            if keep_none:
                set_.append(document)
            else:
                set_.append(delete_none(document))

        return unset, set_, add_to_set
