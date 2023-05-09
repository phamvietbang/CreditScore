import time
from typing import List

from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import DocumentInsertError
from arango.http import DefaultHTTPClient

from config import ArangoDBConfig, get_logger
from constants import ChainConstant, GraphCreditScoreConfigKeys, ArangoIndexConstant, TimeConstant
from model.arango_data_model import KnowledgeGraphModel
from utils.utils import get_connection_elements
from utils.utils import split_token, get_token_query

logger = get_logger('ArangoDB')


class ArangoDbKLG:
    def __init__(self, graph):
        username, password, connection_url = get_connection_elements(graph)
        http_client = DefaultHTTPClient()
        http_client.REQUEST_TIMEOUT = 3600
        self.client = ArangoClient(hosts=connection_url, http_client=http_client)
        self.username = username
        self.password = password
        # self._system_db = self.client.db('_system', username=username, password=password)
        # self.klg_db = self._create_db(ArangoDbConfig.KLG_DATABASE)
        self.klg_db = self._get_db(ArangoDBConfig.KLG_DATABASE)
        self.klg = self._get_graph(ArangoDBConfig.KLG)
        self._wallets_db = self._get_collections(ArangoDBConfig.WALLETS)
        self._multichain_wallets_db = self._get_collections(ArangoDBConfig.MULTICHAIN_WALLETS)
        self._wallet_scores_db = self._get_collections(ArangoDBConfig.WALLET_SCORES)
        self._multichain_wallet_scores_db = self._get_collections(ArangoDBConfig.MULTICHAIN_WALLET_SCORES)
        self._merged_wallet_scores_db = self._get_collections(ArangoDBConfig.MERGED_WALLET_SCORES)
        self._configs_db = self._get_collections(ArangoDBConfig.CREDIT_SCORE_CONFIGS)
        self._multichain_wallets_size_db = self._get_collections(ArangoDBConfig.MULTICHAIN_WALLETS_SIZE)

        self._create_index()

    def _get_db(self, db_name):
        return self.client.db(db_name, username=self.username, password=self.password)

    # def _create_db(self, db_name, system_db: StandardDatabase = None):
    #     if not system_db:
    #         system_db = self._system_db
    #     if not system_db.has_database(db_name):
    #         system_db.create_database(db_name)
    #     return self._get_db(db_name)

    def _create_db(self, db_name):
        return self._get_db(db_name)

    def _get_graph(self, graph_name, edge_definitions=KnowledgeGraphModel.edgeDefinitions,
                   database: StandardDatabase = None):
        if not database:
            database = self.klg_db
        if not database.has_graph(graph_name):
            database.create_graph(graph_name, edge_definitions=edge_definitions)
        return database.graph(graph_name)

    def _get_collections(self, collection_name, database: StandardDatabase = None, edge=False):
        if not database:
            database = self.klg_db
        if not database.has_collection(collection_name):
            database.create_collection(collection_name, edge=edge)
        return database.collection(collection_name)

    def _create_index(self):
        self._multichain_wallets_db.add_hash_index(
            fields=["flagged"], name=ArangoIndexConstant.multichain_wallet_flagged)
        self._multichain_wallet_scores_db.add_hash_index(
            fields=['address'], name=ArangoIndexConstant.multichain_wallet_scores_addresses)
        self._multichain_wallet_scores_db.add_hash_index(
            fields=['flagged'], name=ArangoIndexConstant.multichain_wallet_scores_flagged)

    def get_wallets(self, addresses):
        qry = f"FOR doc IN wallets FILTER doc._key IN {addresses} RETURN doc"
        cursor = self.klg_db.aql.execute(
            qry,
            batch_size=1000
        )
        wallets = list(cursor)

        # results = self._wallets_db.find()
        return wallets

    def update_wallets(self, wallets: List, _type="update"):
        for wallet in wallets:
            wallet["_key"] = f'{wallet.get("chainId")}_{wallet.get("address")}'

        retry_time = 0
        while retry_time < 3:
            try:
                return self._wallets_db.import_bulk(documents=wallets, sync=True, on_duplicate=_type)
            except Exception as ex:
                logger.warning(ex)
                retry_time += 1

    def update_multichain_wallets(self, wallets: List, _type="update"):
        for wallet in wallets:
            wallet["_key"] = wallet.get("address")

        retry_time = 0
        while retry_time < 3:
            try:
                return self._multichain_wallets_db.import_bulk(documents=wallets, sync=True, overwrite=False, on_duplicate=_type)
            except Exception as ex:
                logger.warning(ex)
                retry_time += 1

    def check_exists(self, addresses):
        query = f'FOR w IN multichain_wallets FILTER w._key IN {addresses} RETURN w.address'
        cursor = self.klg_db.aql.execute(query, batch_size=1000)
        return list(cursor)

    def check_exists_wallets(self, addresses):
        keys = []
        for address in addresses:
            keys.extend([f'{chain_id}_{address}' for chain_id in ChainConstant.all])
        query = f'FOR w IN wallets FILTER w._key IN {keys} RETURN {{chain_id: w.chainId, address: w.address, created_at: w.createdAt}}'
        cursor = self.klg_db.aql.execute(query, batch_size=1000)
        return list(cursor)

    def update_properties(self, wallets: List, chain_id):
        for wallet in wallets:
            address = wallet.get('address')
            wallet["_key"] = f"{chain_id}_{address}"
        return self._wallets_db.import_bulk(documents=wallets, sync=True, overwrite=False, on_duplicate="update")

    def update_tokens(self, tokens: List, _type="update"):
        data = {}
        for token in tokens:
            sub_data = split_token(token, merged=False)
            for key, value in sub_data.items():
                if key not in data:
                    data[key] = []
                data[key].append(value)

        for db_name, update_data in data.items():
            try:
                self._get_collections(db_name).import_bulk(documents=update_data, sync=True, on_duplicate=_type)
            except DocumentInsertError as ex:
                logger.warning(ex)
                try:
                    self._get_collections(db_name).import_bulk(documents=update_data, sync=True, on_duplicate=_type)
                except Exception as ex:
                    logger.exception(ex)
            except Exception as ex:
                logger.exception(ex)

    def update_merged_tokens(self, merged_tokens: List, _type="update"):
        data = {}
        for token in merged_tokens:
            sub_data = split_token(token, merged=True)
            for key, value in sub_data.items():
                if key not in data:
                    data[key] = []
                data[key].append(value)

        for db_name, update_data in data.items():
            try:
                self._get_collections(db_name).import_bulk(documents=update_data, sync=True, on_duplicate=_type)
            except DocumentInsertError as ex:
                logger.warning(ex)
                try:
                    self._get_collections(db_name).import_bulk(documents=update_data, sync=True, on_duplicate=_type)
                except Exception as ex:
                    logger.exception(ex)
            except Exception as ex:
                logger.exception(ex)

    def get_token(self, address, chain_id):
        query = get_token_query(_key=f'{chain_id}_{address}', merge=False)
        try:
            cursor = self.klg_db.aql.execute(query)
            data = list(cursor)
            if not data:
                return None
            return data[-1]
        except Exception as ex:
            logger.exception(ex)
        return None

    def get_tokens(self, addresses=None, chain_id=None, batch_size=1000):
        try:
            if addresses is None:
                if chain_id is None:
                    query = get_token_query(merge=False)
                else:
                    query = get_token_query(chain_id=chain_id, merge=False)
            else:
                if chain_id is None:
                    query = get_token_query(addresses=addresses, merge=False)
                else:
                    keys = [chain_id + address for address in addresses]
                    query = get_token_query(_keys=keys, merge=False)

            cursor = self.klg_db.aql.execute(query, batch_size=batch_size, ttl=900)
            return cursor
        except Exception as ex:
            logger.exception(ex)
        return {}

    def get_merged_tokens(self, token_ids=None, exists_score=None):
        try:
            start_time = time.time()
            if exists_score:
                # query = f"FOR doc IN merged_tokens FILTER doc.creditScore > 0 AND doc.priceLastUpdatedAt > {time.time() - 86400} RETURN doc"
                # query = f"FOR doc IN merged_tokens FILTER doc.creditScore > 0 RETURN doc"
                query = get_token_query(filter_=['token.creditScore > 0', f'token.priceLastUpdatedAt > {int(time.time()) - 86400}'], merge=True)
            else:
                if token_ids:
                    # query = f"FOR doc IN merged_tokens FILTER doc._key IN {token_ids} RETURN doc"
                    query = get_token_query(_keys=token_ids, merge=True)
                else:
                    # query = "FOR doc IN merged_tokens RETURN doc"
                    query = get_token_query(filter_=['contains(token, "tokenId")'], merge=True)

            cursor = self.klg_db.aql.execute(query, batch_size=200, ttl=3600)
            logger.info(f'Got cursor took {time.time() - start_time} seconds')
            merged_tokens = {t['tokenId']: t for t in cursor}
            return merged_tokens
        except Exception as ex:
            logger.exception(ex)
            return {}

    def get_merged_tokens_(self, token_ids=None, exists_score=None, batch_size=100):
        try:
            start_time = time.time()
            if token_ids is None:
                if exists_score:
                    filter_ = f'''
                        FILTER token.creditScore > 0
                        FILTER token.priceLastUpdatedAt > {int(time.time()) - 86400}'''
                else:
                    filter_ = ''

                query = f"FOR token IN merged_tokens {filter_} RETURN token._key"
                cursor = self.klg_db.aql.execute(query)
                keys = list(cursor)
                logger.info(f'Get {len(keys)} token keys from graph took {time.time() - start_time} seconds')
            else:
                keys = token_ids

            merged_tokens = {}
            for idx in range(0, len(keys), batch_size):
                sub_keys = keys[idx:idx + batch_size]
                query = get_token_query(_keys=sub_keys, merge=True)
                cursor = self.klg_db.aql.execute(query, ttl=900)
                merged_tokens.update({t['tokenId']: t for t in cursor})
                logger.info(f'Load {len(merged_tokens)} tokens took {time.time() - start_time} seconds')

            return merged_tokens
        except Exception as ex:
            logger.exception(ex)
        return {}

    def get_tokens_statistic_field(self):
        query = """
            FOR doc IN tokens 
                FOR ttv IN token_trading_volumes
                    FILTER doc._key == ttv._key 
            RETURN {
                address: doc.address, 
                daily_number_of_transactions: doc.dailyNumberOfTransactions, 
                number_of_holder: doc.numberOfHolder, 
                daily_trading_volumes: ttv.dailyTradingVolumes, 
                market_cap: doc.marketCap, 
                holder_distribution: doc.holderDistribution, 
                trading_volume_24h: doc.tradingVolume24h
            }
        """
        cursor = self.klg_db.aql.execute(query, batch_size=1000, ttl=3600)
        tokens = list(cursor)
        return tokens

    def get_tokens_credit_score(self):
        start_time = time.time()
        query = "FOR t IN tokens " \
                "RETURN {_key: t._key, creditScore: t.creditScore, price: t.price}"
        cursor = self.klg_db.aql.execute(
            query,
            batch_size=1000
        )

        data = list(cursor)
        tokens = {}
        for token in data:
            _key = token['_key']
            tokens[_key] = {'score': token['creditScore'] or 0, 'price': token['price'] or 0}
            # tokens[_key] = token['creditScore'] or 0

        logger.info(f'Load {len(tokens)} tokens score take {time.time() - start_time} seconds')
        return tokens

    def get_wallets_lending(self, pool_address=None, batch_size=1000):
        if pool_address:
            query = f'FOR w IN wallets FILTER CONTAINS(w.lendings, "{pool_address}") RETURN w'
        else:
            query = '''
            FOR w IN wallets
                FILTER w.depositInUSD > 0
                RETURN w
            '''
        cursor = self.klg_db.aql.execute(query, batch_size=batch_size)
        return cursor

    def get_multichain_wallets_lending(self, pool_address, chain_id, batch_size=10000):
        pool = f'{chain_id}_{pool_address}'
        query = f'''
        FOR wallet IN multichain_wallets
            FILTER CONTAINS(wallet.lendings, '{pool}')
            RETURN wallet
        '''
        cursor = self.klg_db.aql.execute(query, batch_size=batch_size)
        return cursor

    def get_wallets_with_batch(self, skip=0, batch_size=50000, chain_id=None):
        start_time = time.time()
        if chain_id is None:
            query = f"FOR w IN wallets " \
                    f"FILTER w.flagged == False " \
                    f"LIMIT {skip}, {batch_size} " \
                    f"RETURN w"
            cursor = self.klg_db.aql.execute(
                query,
                batch_size=50000
            )
        else:
            query = f"FOR w IN wallets " \
                    f"FILTER w.flagged == False && w.chainId == \'{chain_id}\' " \
                    f"LIMIT {skip}, {batch_size} " \
                    f"  RETURN w"
            cursor = self.klg_db.aql.execute(
                query,
                batch_size=50000
            )

        wallets = list(cursor)

        logger.info(f'Load {len(wallets)} wallets take {time.time() - start_time} seconds')
        return wallets

    def get_multichain_wallets_with_batch(self, skip=0, batch_size=50000):
        start_time = time.time()
        query = f"FOR w IN multichain_wallets " \
                f"FILTER w.flagged == False " \
                f"LIMIT {skip}, {batch_size} " \
                f"RETURN w"
        cursor = self.klg_db.aql.execute(
                query,
                batch_size=50000
        )

        wallets = list(cursor)

        logger.info(f'Load {len(wallets)} wallets take {time.time() - start_time} seconds')
        return wallets

    def get_multichain_wallets(self):
        start_time = time.time()
        query = f"FOR w IN multichain_wallets " \
                f"RETURN w"
        cursor = self.klg_db.aql.execute(
                query,
                batch_size=50000
        )

        wallets = list(cursor)

        logger.info(f'Load {len(wallets)} wallets take {time.time() - start_time} seconds')
        return wallets

    def get_wallets_with_batch_idx(self, batch_idx=1, batch_size=10000, multichain=True, chain_id=None):
        start_time = time.time()
        if multichain:
            query = f"FOR w IN multichain_wallets " \
                    f"FILTER w.flagged == {batch_idx} " \
                    f"RETURN w"
        else:
            query = f"FOR w IN wallets FILTER w.chainId == '{chain_id}' AND w.flagged == {batch_idx} RETURN w"
        cursor = self.klg_db.aql.execute(query, batch_size=batch_size)
        logger.info(f'Load cursor take {time.time() - start_time} seconds')
        return cursor

    def get_wallets_score_with_batch_idx(self, batch_idx=1, batch_size=10000, multichain=True, chain_id=None):
        start_time = time.time()
        if multichain:
            query = f"FOR w IN multichain_wallets_credit_scores " \
                    f"FILTER w.flagged == {batch_idx} " \
                    f"RETURN w"
        else:
            query = f"FOR w IN wallets_credit_scores FILTER w.chainId == '{chain_id}' AND w.flagged == {batch_idx} RETURN w"
        cursor = self.klg_db.aql.execute(query, batch_size=batch_size)
        logger.info(f'Load cursor take {time.time() - start_time} seconds')
        return cursor

    def get_root_wallets(self, batch_size=10000):
        query = f"FOR w IN multichain_wallets RETURN w"
        cursor = self.klg_db.aql.execute(query, batch_size=batch_size)
        return cursor

    def get_asset_change_logs_(self, _type, chain_id=None, skip=0, limit=100000):
        fields = [f'{_type}InUSD: w.{_type}InUSD', f'{_type}ChangeLogs: w.{_type}ChangeLogs']
        wallets = []

        if chain_id is None:
            query = f"FOR w IN wallets " \
                    f"LIMIT {skip}, {limit} " \
                    f"RETURN " + "{ address: w.address, " + ', '.join(fields) + "}"
            data = self.klg_db.aql.execute(query)
        else:
            query = f"FOR w IN wallets " \
                    f"FILTER w.chainId == \'{chain_id}\'" \
                    f"LIMIT {skip}, {limit} " \
                    f"RETURN " + "{ address: w.address, " + ', '.join(fields) + "}"
            data = self.klg_db.aql.execute(query)

        data = list(data)

        for w in data:
            # Keys in ArangoDB is string, so we need to convert to integer first.
            timestamps = list(w.get(f'{_type}ChangeLogs', {}).keys())
            timestamps = [int(timestamp) for timestamp in timestamps]
            values = list(w.get(f'{_type}ChangeLogs', {}).values())

            wallets.append({
                'address': w['address'],
                'usd': w[f'{_type}InUSD'] or 0,
                'timestamps': timestamps,
                'values': values
            })

        return wallets

    def get_wallet_statistic_field_(self, field, chain_id=None, skip=0, limit=100000):
        if chain_id is None:
            query = f"FOR w IN wallets " \
                    f"LIMIT {skip}, {limit} " \
                    f"RETURN w.{field}"
            data = self.klg_db.aql.execute(query)
        else:
            query = f"FOR w IN wallets " \
                    f"FILTER w.chainId == \'{chain_id}\' " \
                    f"LIMIT {skip}, {limit} " \
                    f"RETURN w.{field}"
            data = self.klg_db.aql.execute(query)

        wallets = list(data)
        return wallets

    def get_has_merged_tokens(self, token_id):
        try:
            query = f"""
                WITH tokens
                FOR m IN merged_tokens
                    FILTER m._key == '{token_id}'
                    FOR t IN 1..1 INBOUND m has_merged_tokens
                        RETURN {{
                            chain_id: t.chainId,
                            address: t.address
                        }}
            """
            cursor = self.klg_db.aql.execute(query, batch_size=1000)
            tokens = list(cursor)
            return tokens
        except Exception as ex:
            logger.exception(ex)
            return []

    def get_tokens_id(self, chain_id=None):
        try:
            if chain_id:
                query = f"FOR doc IN tokens FILTER doc.chainId == '{chain_id}' RETURN {{token_id: doc.tokenId, address: doc.address, chain_id: doc.chainId}}"
            else:
                query = f"FOR doc IN tokens RETURN {{token_id: doc.tokenId, address: doc.address, chain_id: doc.chainId}}"
            cursor = self.klg_db.aql.execute(query, batch_size=1000)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
            return []

    def get_wallet_addresses(self, chain_id=None, batch_size=100000, update_created_at=False):
        try:
            if chain_id:
                query = f"FOR doc IN wallets FILTER doc.chainId == '{chain_id}' RETURN doc.address"
            else:
                if update_created_at:
                    query = "FOR doc IN multichain_wallets FILTER NOT HAS(doc, 'updatedCreatedAt') RETURN doc.address"
                else:
                    query = "FOR doc IN multichain_wallets RETURN doc.address"

            cursor = self.klg_db.aql.execute(query, batch_size=batch_size)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
            return []

    def get_top_tokens(self, n_top_tokens=100):
        try:
            query = f"""
                FOR t IN merged_tokens 
                FILTER 'Compound Tokens' NOT IN t.categories
                FILTER t.priceLastUpdatedAt > {int(time.time() - TimeConstant.A_DAY)}
                SORT t.marketCap DESC 
                LIMIT {n_top_tokens} 
                LET market_cap = (
                    FOR market_cap IN merged_token_market_caps
                    FILTER market_cap._key == t._key
                    RETURN market_cap
                )
                LET trading_volume = (
                    FOR trading_volume IN merged_token_trading_volumes
                    FILTER trading_volume._key == t._key
                    RETURN trading_volume
                )
                LET price = (
                    FOR price IN merged_token_price
                    FILTER price._key == t._key
                    RETURN price
                )
                RETURN {{
                    tokenId: t.tokenId,
                    marketCapChangeLogs: market_cap[0].marketCapChangeLogs,
                    priceChangeLogs: price[0].priceChangeLogs, 
                    dailyTradingVolumes: trading_volume[0].dailyTradingVolumes
                }}
            """
            cursor = self.klg_db.aql.execute(query, batch_size=1000)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
            return []

    def get_wallet_scores_change_logs(self, chain_id=None, token_address=None, batch_idx=1, batch_size=50000):
        try:
            if chain_id is not None:
                if token_address is not None:
                    token_key = f'{chain_id}_{token_address}'
                    query = f"""
                        FOR w IN multichain_wallets 
                        FILTER w.tokens['{token_key}'] > 0 
                        RETURN w._key
                    """
                    cursor = self.klg_db.aql.execute(query, batch_size=50000)
                    addresses = list(cursor)

                    query = f"""
                        FOR w IN multichain_wallets_credit_scores 
                        FILTER w._key IN {addresses}
                        RETURN {{
                            address: w.address,
                            history: w.creditScoreChangeLogs
                        }}
                    """
                else:
                    query = f"""
                        FOR w IN wallets_credit_scores
                        FILTER w.chainId == '{chain_id}'
                        FILTER w.flagged == {batch_idx}
                        RETURN {{
                            address: w.address,
                            history: w.creditScoreChangeLogs
                        }}
                    """
            else:
                query = f"""
                    FOR w IN multichain_wallets_credit_scores 
                    FILTER w.flagged == {batch_idx}
                    RETURN {{
                        address: w.address,
                        history: w.creditScoreChangeLogs
                    }}
                """
            cursor = self.klg_db.aql.execute(query, batch_size=batch_size)
            return cursor
        except Exception as ex:
            logger.exception(ex)
            return None

    def get_number_of_wallets(self, chain_id=None):
        if chain_id is None:
            query = "RETURN COUNT(FOR w IN multichain_wallets RETURN 1)"
        else:
            query = f"RETURN COUNT(FOR w IN wallets FILTER w.chainId == '{chain_id}' RETURN 1)"
        cursor = self.klg_db.aql.execute(query)
        return int(list(cursor.batch())[-1])

    def get_wallet(self, address, chain_id=None):
        if chain_id is None:
            query = f"FOR w IN multichain_wallets FILTER w._key == '{address}' RETURN w"
        else:
            key = f'{chain_id}_{address}'
            query = f"FOR w IN wallets FILTER w._key == '{key}' RETURN w"
        cursor = self.klg_db.aql.execute(query, batch_size=1)
        wallets = list(cursor.batch())
        if not wallets:
            return None
        return wallets[-1]

    def get_wallet_score(self, address):
        query = f"FOR w IN multichain_wallets_credit_scores FILTER w._key == '{address}' RETURN w"

        cursor = self.klg_db.aql.execute(query, batch_size=1)
        wallets = list(cursor.batch())
        if not wallets:
            return None
        return wallets[-1]

    def get_merged_wallet(self, merged_id):
        query = f"FOR w IN merged_wallets FILTER w._key == '{merged_id}' RETURN w"
        cursor = self.klg_db.aql.execute(query, batch_size=1)
        wallets = list(cursor.batch())
        if not wallets:
            return None
        return wallets[-1]

    def get_part_wallets(self, address):
        try:
            query = f"FOR w IN wallets FILTER w.address == '{address}' RETURN w.chainId"
            cursor = self.klg_db.aql.execute(query, batch_size=10)
            return list(cursor)
        except Exception as ex:
            logger.exception(ex)
            return []

    def get_wallets_with_flagged(self, batch_size=50000, time_to_live=7200, chain_id=None, reset=False):
        if chain_id is None:
            if not reset:
                query = "FOR w IN multichain_wallets FILTER w.flagged < 1 RETURN w.address"
            else:
                query = "FOR w IN multichain_wallets RETURN w.address"
        else:
            if not reset:
                query = f"FOR w IN wallets FILTER w.chainId == '{chain_id}' AND w.flagged < 1 RETURN w.address"
            else:
                query = f"FOR w IN wallets FILTER w.chainId == '{chain_id}' RETURN w.address"

        cursor = self.klg_db.aql.execute(
            query,
            batch_size=batch_size,
            ttl=time_to_live
        )
        return cursor

    def update_multichain_wallet_scores(self, wallet_scores, _type='update'):
        for wallet in wallet_scores:
            wallet["_key"] = wallet.get("address")

        retry_time = 0
        while retry_time < 3:
            try:
                return self._multichain_wallet_scores_db.import_bulk(
                    documents=wallet_scores,
                    sync=True,
                    on_duplicate=_type
                )
            except Exception as ex:
                logger.warning(ex)
                retry_time += 1

    def update_wallet_scores(self, wallet_scores, _type='update'):
        for wallet in wallet_scores:
            wallet["_key"] = f'{wallet["chain_id"]}_{wallet["address"]}'

        retry_time = 0
        while retry_time < 3:
            try:
                return self._wallet_scores_db.import_bulk(
                    documents=wallet_scores,
                    sync=True,
                    on_duplicate=_type
                )
            except Exception as ex:
                logger.warning(ex)
                retry_time += 1

    def update_wallets_size(self, wallets_size, multichain=False):
        retry_time = 0
        while retry_time < 3:
            try:
                if multichain:
                    return self._multichain_wallets_size_db.import_bulk(
                        documents=wallets_size,
                        sync=True,
                        on_duplicate='update'
                    )
                else:
                    return
            except Exception as ex:
                logger.warning(ex)
                retry_time += 1

    def update_merged_wallet_scores(self, wallet_scores, _type='update'):
        for wallet in wallet_scores:
            wallet["_key"] = wallet_scores['mergedWalletId']

        retry_time = 0
        while retry_time < 3:
            try:
                return self._merged_wallet_scores_db.import_bulk(
                    documents=wallet_scores,
                    sync=True,
                    on_duplicate=_type
                )
            except Exception as ex:
                logger.warning(ex)
                retry_time += 1

    def update_configs(self, configs, _type="update"):
        retry_time = 0
        while retry_time < 3:
            try:
                return self._configs_db.import_bulk(documents=configs, sync=True, on_duplicate=_type)
            except Exception as ex:
                logger.warning(ex)
                retry_time += 1

    def get_wallet_statistics(self):
        query = f"""
            FOR doc IN credit_score_configs 
            FILTER doc._key == '{GraphCreditScoreConfigKeys.wallet_statistics}' 
            RETURN {{
                total_asset: doc.total_asset,
                age_of_account: doc.age_of_account,
                transaction_amount: doc.transaction_amount,
                frequency_of_transaction: doc.frequency_of_transaction,
                deposit: doc.deposit,
                borrow: doc.borrow
            }} 
        """
        cursor = self.klg_db.aql.execute(query, batch_size=1)
        config = list(cursor.batch())
        if not config:
            return None
        return config[-1]

    def get_token_statistics(self):
        query = f"FOR doc IN credit_score_configs FILTER doc._key == '{GraphCreditScoreConfigKeys.token_statistics}' RETURN doc"
        cursor = self.klg_db.aql.execute(query, batch_size=1)
        config = list(cursor.batch())
        if not config:
            return None
        return config[-1]

    def get_monitor_days(self):
        query = f"FOR doc IN credit_score_configs FILTER doc._key == '{GraphCreditScoreConfigKeys.monitor_day}' RETURN {{k: doc.k}}"
        cursor = self.klg_db.aql.execute(query, batch_size=1)
        config = list(cursor.batch())
        if not config:
            return 30
        return config[-1]['k']

    def get_wallet_flagged_state(self, chain_id=None):
        if chain_id is None:
            key = GraphCreditScoreConfigKeys.multichain_wallets_flagged_state
        else:
            key = GraphCreditScoreConfigKeys.wallets_flagged_state + '_' + chain_id

        query = f"FOR doc IN credit_score_configs FILTER doc._key == '{key}' RETURN doc"
        cursor = self.klg_db.aql.execute(query, batch_size=1)
        config = list(cursor.batch())
        if not config:
            return None
        return config[-1]

    def get_multichain_wallet_with_addresses(self, addresses, batch_size=1000):
        query = f"""
            FOR w IN multichain_wallets
            FILTER w._key IN {addresses}
            RETURN w
        """
        cursor = self.klg_db.aql.execute(query, batch_size=batch_size, ttl=3600)
        return cursor

    def get_elite_wallets(self, batch_size=1000):
        query = """
            FOR doc IN multichain_wallets
            FILTER doc.elite
            RETURN doc
        """
        cursor = self.klg_db.aql.execute(query, batch_size=batch_size)
        return cursor
