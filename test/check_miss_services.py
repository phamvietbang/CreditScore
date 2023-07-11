import json

from database.mongodb import MongoDB
from database.arangodb_klg import ArangoDbKLG


def check_miss(mongo: MongoDB, arangodb: ArangoDbKLG, from_block: int, to_block: int, chain_id: str, file: str, event_types: list):
    events = mongo.get_documents('lending_events', {'block_number': {"$gte": from_block, "$lte": to_block},"event_type":{'$in':event_types}})
    wallets, timestamps = [], {}
    miss_events = {}
    for event in events:
        wallet = event['wallet']
        timestamp = event['block_timestamp']
        if wallet not in timestamps:
            timestamps[wallet] = []

        timestamps[wallet].append(timestamp)
        miss_events[f'{timestamp}_{wallet}'] = {
            '_id': event['_id'],
            'transaction_hash': event['transaction_hash'],
        }
        wallets.append(wallet)

    graph_wallets = arangodb.get_wallets(addresses=[f'{chain_id}_{i}' for i in wallets])
    miss_event_list = []
    for wallet in graph_wallets:
        address = wallet['address']
        deposit_change_logs = wallet['depositChangeLogs']
        for timestamp in timestamps[address]:
            if str(timestamp) not in deposit_change_logs:
                if miss_events[f'{timestamp}_{address}'] in miss_event_list: continue
                miss_event_list.append(miss_events[f'{timestamp}_{address}'])
    with open(file, 'w') as f:
        f.write(json.dumps({'count': len(miss_event_list), 'miss_event': miss_event_list}))

    return miss_event_list


if __name__ == '__main__':
    event_types = ['DEPOSIT', 'BORROW', 'WITHDRAW', 'REPAY', 'LIQUIDATE']
    mongo = MongoDB('', 'blockchain_etl', 'bsc')
    arango = ArangoDbKLG('')
    data = check_miss(mongo, arango, 58829350, 59051268, '0xfa', '1_1_1.json', event_types)
