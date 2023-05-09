import json
import time

from pymongo import MongoClient

from config.config import AgeDBConfig, get_logger
from config.constant import Neo4jWalletConstant, WalletConstant, ChainConstant
from utils.response import ApiBadRequest

logger = get_logger('Age Service')


# def update_wallet_age(input_wallet_addresses, mongodb, graph, chain_id):
#     updated_age_wallets = mongodb.get_wallets(input_wallet_addresses)
#     updated_age_wallet_addresses = [wallet.get("address") for wallet in updated_age_wallets]
#     wallet_nodes = graph.get_list_wallets(updated_age_wallet_addresses)
#     updated_nodes = list()
#     for wallet in updated_age_wallets:
#         updated_node = wallet_nodes.get(wallet.get("address"))
#         if updated_node:
#             updated_node[Neo4jWalletConstant.createdAt] = wallet.get("created_at")
#             updated_nodes.append(updated_node)
#     graph.update_wallets(updated_nodes)


def update_age(address, age_db, graph, chain_id):
    # wallet_node = graph.get_wallet(address, chain_id=chain_id)
    wallet_nodes = graph.get_wallet_with_root(address, chain_id=chain_id)
    exists = True
    if not wallet_nodes:
        exists = False

    wallet = age_db.get_wallet(address, chain_id=chain_id)
    if wallet is None:
        logger.info(f'Wallet {address} not exists in Age DB')
        return None, exists

    address = wallet.get("address")
    updated_node = {
        Neo4jWalletConstant.chain_id: chain_id,
        Neo4jWalletConstant.address: address,
        Neo4jWalletConstant.createdAt: wallet.get('created_at')
    }

    if wallet_nodes and wallet_nodes.get('root_wallet'):
        root_wallet = wallet_nodes.get('root_wallet')
        root_created_at = root_wallet.get('createdAt')
        if wallet.get('created_at') < root_created_at:
            updated_root_node = {
                Neo4jWalletConstant.address: address,
                Neo4jWalletConstant.createdAt: wallet.get('created_at')
            }
            graph.update_root_wallet(updated_root_node)
    graph.update_wallet(updated_node)
    return wallet.get('created_at'), exists


def update_multichain_wallet_age(address, created_at, age_db, graph, exists=True):
    wallet_created_at, created_ats = age_db.wallet_created_at(address, created_at)
    if wallet_created_at is None:
        return None

    if not exists:
        for w in created_ats:
            w.update({'newAddress': True})
    else:
        chains = graph.get_part_wallets(address)
        for w in created_ats:
            if w['chainId'] not in chains:
                w['newAddress'] = True

    for w in created_ats:
        w.update({'updatedCreatedAt': True})

    graph.update_wallets(created_ats)
    graph.update_multichain_wallets([{
        'address': address,
        'createdAt': wallet_created_at,
        'updatedCreatedAt': True
    }])
    return wallet_created_at


def update_wallet_ages(mongodb, graph, addresses=None, save=True, batch_size=100000):
    start_time = time.time()
    if addresses is None:
        addresses = graph.get_wallet_addresses(batch_size=batch_size, update_created_at=True)
    else:
        addresses = graph.check_exists(addresses)

    sub_addresses = []
    batch_idx = 0
    for address in addresses:
        sub_addresses.append(address)

        if len(sub_addresses) >= batch_size:
            batch_idx += 1
            execute_batch(mongodb, graph, sub_addresses, batch_idx, save)
            sub_addresses = []

    if sub_addresses:
        execute_batch(mongodb, graph, sub_addresses, batch_idx + 1, save)
    logger.info(f'Total time: {time.time() - start_time} seconds')


def execute_batch(mongodb, graph, sub_addresses, batch_idx, save):
    start_time = time.time()
    logger.info(f'Execute batch {batch_idx} with {len(sub_addresses)} wallets')
    wallet_exists = graph.check_exists_wallets(sub_addresses)
    wallets_in_chain = {chain: [] for chain in ChainConstant.all}
    for w in wallet_exists:
        wallets_in_chain[w['chain_id']].append(w['address'])

    wallet_ages = {}
    cnt = 0
    for chain in ChainConstant.all:
        ages = mongodb.get_wallets(wallets_in_chain[chain], chain_id=chain)
        cnt += len(ages)
        wallet_ages[chain] = {w['address']: w.get('created_at') for w in ages}
        for w in wallet_exists:
            if (w['chain_id'] == chain) and (w['address'] not in wallet_ages[chain]):
                wallet_ages[chain][w['address']] = w.get('created_at') or int(time.time())
    logger.info(f'Number of onechain wallets: {cnt} - {time.time() - start_time} seconds')

    update_nodes = list()
    updated_multichain_nodes = list()
    for address in sub_addresses:
        created_at = None
        for chain in ChainConstant.all:
            tmp = wallet_ages[chain].get(address)
            if tmp:
                update_nodes.append({
                    'address': address,
                    'chainId': chain,
                    'createdAt': int(tmp),
                    'updatedCreatedAt': True
                })
                if (created_at is None) or (tmp < created_at):
                    created_at = tmp
        if created_at:
            updated_multichain_nodes.append({
                'address': address,
                'createdAt': int(created_at),
                'updatedCreatedAt': True
            })
    logger.info(f'Updated Node: {len(updated_multichain_nodes)} wallets')

    if save:
        graph.update_multichain_wallets(updated_multichain_nodes)
        with open('data/multichain_wallets_created_at.json', 'w') as f:
            json.dump(updated_multichain_nodes, f)
        logger.info(f'Updated multichain wallet ages {len(updated_multichain_nodes)} take {time.time() - start_time} seconds')

        graph.update_wallets(update_nodes)
        with open('data/wallets_created_at.json', 'w') as f:
            json.dump(update_nodes, f)
        logger.info(f'Updated wallet ages {len(update_nodes)} take {time.time() - start_time} seconds')
    else:
        logger.info(f'Created At of multichain wallets: {updated_multichain_nodes}')
        logger.info(f'Created At of wallets: {update_nodes}')


class AgeDB:
    def __init__(self, url=None, timeout=10000):
        if url is None:
            url = f"mongodb://{AgeDBConfig.USERNAME}:{AgeDBConfig.PASSWORD}@{AgeDBConfig.HOST}:{AgeDBConfig.PORT}"
        connection = MongoClient(url, serverSelectionTimeoutMS=timeout)
        self.bsc_db = connection[AgeDBConfig.BSC_DB]
        self.bsc_wallets_collection = self.bsc_db[AgeDBConfig.WALLET_COLLECTION]
        self.ethereum_db = connection[AgeDBConfig.ETHEREUM_DB]
        self.ethereum_wallets_collection = self.ethereum_db[AgeDBConfig.WALLET_COLLECTION]
        self.polygon_db = connection[AgeDBConfig.POLYGON_DB]
        self.polygon_wallets_collection = self.polygon_db[AgeDBConfig.WALLET_COLLECTION]
        self.ftm_db = connection[AgeDBConfig.FTM_DB]
        self.ftm_wallets_collection = self.ftm_db[AgeDBConfig.WALLET_COLLECTION]

        # Set index for collection
        self.bsc_wallets_collection.create_index('address')
        self.ethereum_wallets_collection.create_index('address')
        self.ftm_wallets_collection.create_index('address')
        self.polygon_wallets_collection.create_index('address')

        self.mapping = {
            ChainConstant.bsc_chain_id: self.bsc_db,
            ChainConstant.eth_chain_id: self.ethereum_db,
            ChainConstant.ftm_chain_id: self.ftm_db,
            ChainConstant.polygon_chain_id: self.polygon_db
        }

    def get_wallets(self, wallet_addresses=None, chain_id=ChainConstant.bsc_chain_id):
        if wallet_addresses:
            filter_ = {
                WalletConstant.address: {"$in": wallet_addresses}
            }
        else:
            return []

        if chain_id not in self.mapping:
            return None

        wallet_collection = self.mapping[chain_id][AgeDBConfig.WALLET_COLLECTION]
        result = wallet_collection.find(filter_)
        result = list(result)
        return result

    def get_wallet(self, address, chain_id=ChainConstant.bsc_chain_id):
        if chain_id not in self.mapping:
            return None

        wallet_collection = self.mapping[chain_id][AgeDBConfig.WALLET_COLLECTION]
        wallet = wallet_collection.find_one({WalletConstant.address: address})
        return wallet

    def get_wallet_created_at(self, chain_id, address):
        if chain_id not in self.mapping:
            return None

        mongo_wallets = self.mapping[chain_id][AgeDBConfig.WALLET_COLLECTION]
        wallet = mongo_wallets.find_one({'address': address})
        if not wallet:
            return None
        return wallet.get('created_at')

    def get_all_top_token(self):
        tokens = {}
        for chain_id, mongo_top_tokens in self.mapping.items():
            top_tokens = mongo_top_tokens[AgeDBConfig.TOP_TOKENS].find({}, ['address'])
            if top_tokens:
                tokens[chain_id] = list(top_tokens)
        return tokens

    def add_tokens(self, tokens, chain_id):
        if chain_id not in self.mapping:
            raise ApiBadRequest(f'Not support tokens for chain {chain_id}')

        token_collection = self.mapping[chain_id][AgeDBConfig.TOP_TOKENS]
        token_collection.insert_many(tokens)

    def wallet_created_at(self, address, created_at):
        try:
            created_ats = []
            wallet_created_at = created_at
            for chain_id, db in self.mapping.items():
                wallet_collection = db[AgeDBConfig.WALLET_COLLECTION]
                wallet = wallet_collection.find_one({'address': address})
                if wallet:
                    created_ats.append({'chainId': chain_id, 'address': address, 'createdAt': wallet['created_at']})
                    if (not wallet_created_at) or (wallet['created_at'] < wallet_created_at):
                        wallet_created_at = wallet['created_at']

            return wallet_created_at, created_ats
        except Exception as ex:
            logger.exception(ex)
        return None, []


# if __name__ == '__main__':
#     mongodb_url = f"mongodb://{AgeDBConfig.USERNAME}:{AgeDBConfig.PASSWORD}@{AgeDBConfig.HOST}:{AgeDBConfig.PORT}"
#     mongo_db = AgeDB(mongodb_url)
#     graph_uri = f'{Neo4jConfig.BOLT}@{Neo4jConfig.NEO4J_USERNAME}:{Neo4jConfig.NEO4J_PASSWORD}'
#     graph_db = KLGraph(graph_uri)
#
#     wallet_addresses_ = ["0x2ae9ada4a84b8b1c31639546f3fff91eb786647a", "0x72af20bdae54756576b3725e73b75391e599a191"]
#     update_wallet_age(wallet_addresses_, mongodb=mongo_db, graph=graph_db)
