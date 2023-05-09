from backtest.charts.decorators import Decorator
from database.arangodb_klg import ArangoDbKLG
from database.mongodb import MongoDB


class LiquidateDataProcessors:
    def __init__(self, arango_klg: ArangoDbKLG, mongo_db: MongoDB, chain_id: str):
        self.chain_id = chain_id
        self.arango_klg = arango_klg
        self.mongo_db = mongo_db

    @staticmethod
    def get_bna_liquidation_score(cursor):
        time_con = {}
        addresses = []
        for i in cursor:
            time_con[i['address']] = {
                'bf': i['scoringTimestamps']['before_liquidate'],
                'af': i['scoringTimestamps']['in_liquidate']
            }
            addresses.append(i['address'])
        return time_con, addresses

    @staticmethod
    def get_bna_liquidation_amount(cursor, time_con):
        balance = {}
        deposit = {}
        borrow = {}
        for i in cursor:
            for token in i["tokenChangeLogs"]:
                if token not in balance:
                    balance[token] = {'bf': 0, 'af': 0}
                balance1, tmp1, tmp2 = 0, 0, 0
                for timestamp in i["tokenChangeLogs"][token]:
                    if time_con[i['address']]['bf'] >= int(timestamp) >= tmp1:
                        balance1 = i["tokenChangeLogs"][token][timestamp]['valueInUSD']
                        tmp1 = int(timestamp)
                balance[token]['bf'] += balance1
                balance2 = balance1
                for timestamp in i["tokenChangeLogs"][token]:
                    if int(timestamp) >= time_con[i['address']]['af'] and int(timestamp) >= tmp2:
                        balance2 = i["tokenChangeLogs"][token][timestamp]['valueInUSD']
                        tmp2 = int(timestamp)

                balance[token]['af'] += balance2

            for token in i["depositTokenChangeLogs"]:
                if token not in deposit:
                    deposit[token] = {'bf': 0, 'af': 0}
                tmp1, tmp2 = 0, 0
                deposit1, deposit2 = 0, 0
                for timestamp in i["depositTokenChangeLogs"][token]:
                    if time_con[i['address']]['bf'] >= int(timestamp) >= tmp1:
                        deposit1 = i["depositTokenChangeLogs"][token][timestamp]['valueInUSD']
                        tmp1 = int(timestamp)
                deposit[token]['bf'] += deposit1
                deposit2 = deposit1
                for timestamp in i["depositTokenChangeLogs"][token]:
                    if int(timestamp) >= time_con[i['address']]['af'] and int(timestamp) >= tmp2:
                        deposit2 = i["depositTokenChangeLogs"][token][timestamp]['valueInUSD']
                        tmp2 = int(timestamp)
                deposit[token]['af'] += deposit2
            for token in i["borrowTokenChangeLogs"]:
                if token not in borrow:
                    borrow[token] = {'bf': 0, 'af': 0}
                tmp1, tmp2 = 0, 0
                borrow1, borrow2 = 0, 0
                for timestamp in i["borrowTokenChangeLogs"][token]:
                    if time_con[i['address']]['bf'] >= int(timestamp) >= tmp1:
                        borrow1 = i["borrowTokenChangeLogs"][token][timestamp]['valueInUSD']
                        tmp1 = int(timestamp)
                borrow[token]['bf'] += borrow1
                borrow2 = borrow1
                for timestamp in i["borrowTokenChangeLogs"][token]:
                    if int(timestamp) >= time_con[i['address']]['af'] and int(timestamp) >= tmp2:
                        borrow2 = i["borrowTokenChangeLogs"][token][timestamp]['valueInUSD']
                        tmp2 = int(timestamp)
                borrow[token]['af'] += borrow2

        return balance, deposit, borrow

    def calculate_bna_liquidation_amount_addresses(self, addresses):
        cursor = self.arango_klg.get_multichain_wallet_with_addresses(addresses)
        time_con, addresses = self.get_bna_liquidation_score(cursor)
        cursor = self.arango_klg.get_wallets([f'{self.chain_id}_{i}' for i in addresses])
        return self.get_bna_liquidation_amount(cursor, time_con)

    def calculate_bna_liquidation_amount(self):
        cursor = self.arango_klg.get_multichain_wallets()
        time_con, addresses = self.get_bna_liquidation_score(cursor)
        cursor = self.arango_klg.get_wallets([f'{self.chain_id}_{i}' for i in addresses])
        return self.get_bna_liquidation_amount(cursor, time_con)

    def calculate_liquidate_debt_amount(self, timestamps, from_timestamp, end_timestamp, folks=None):
        data = self.mongo_db.get_documents("debtors", {})
        debt_amount = []
        result = {}
        total_amount = 0
        for doc in data:
            for address, value in doc["buyers"].items():
                reduce_value = Decorator.reduce_event(value, 'debt')
                for timestamp, obj in reduce_value.items():
                    total_amount += obj['debtAssetInUSD']
                    key = Decorator.check_timestamp(timestamp, timestamps)
                    if int(key) < from_timestamp or int(key) > end_timestamp: continue
                    if folks and obj['protocol'] not in folks: continue
                    debt_asset = obj['debtAsset']
                    if debt_asset not in result:
                        result[debt_asset] = {
                            "amount": 0,
                            "number": 0
                        }
                    debt_amount.append(obj['debtAssetInUSD'])
                    result[debt_asset]['number'] += 1
                    result[debt_asset]['amount'] += obj['debtAssetInUSD']
        debt_amount.sort()
        return total_amount, debt_amount[0], debt_amount[1], result

    def calculate_liquidate_collateral_amount(self, timestamps, from_timestamp, end_timestamp, folks=None):
        data = self.mongo_db.get_documents("debtors", {})
        total_amount = 0
        result = {}
        coll_amount = []
        for doc in data:
            for address, value in doc["debtors"].items():
                reduce_value = Decorator.reduce_event(value, 'collateral')
                for timestamp, obj in reduce_value.items():
                    key = Decorator.check_timestamp(timestamp, timestamps)
                    if int(key) < from_timestamp or int(key) > end_timestamp: continue
                    if folks and obj['protocol'] not in folks: continue
                    debt_asset = obj['collateralAsset']
                    if debt_asset not in result:
                        result[debt_asset] = {
                            "amount": 0,
                            "number": 0
                        }
                    coll_amount.append(obj['collateralAssetInUSD'])
                    result[debt_asset]['number'] += 1
                    result[debt_asset]['amount'] += obj['collateralAssetInUSD']
        coll_amount.sort()
        return total_amount, coll_amount[0], coll_amount[1], result
