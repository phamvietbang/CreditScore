import json
import time
import datetime

import pandas as pd

from database.mongodb import MongoDB
from database.mongodb_klg import MongoDBKLG
from utils.time_utils import round_timestamp


class KLGProcessor:
    def __init__(self, mongodb: MongoDB, local_klg: MongoDB, main_klg: MongoDBKLG):
        self.mongodb = mongodb
        self.local_klg = local_klg
        self.main_klg = main_klg

    def fix_multichain_data(self):
        cursor = self.local_klg.get_documents("multichain_wallets", {})
        for i in cursor:
            data = {
                "_id": i["_id"],
                "dailyNumberOfTransactions": {},
                "dailyTransactionAmounts": {}
            }
            for timestamp, value in i['dailyNumberOfTransactionsInEachChain'].items():
                data["dailyNumberOfTransactions"][timestamp] = sum(value.values())
            for timestamp, value in i['dailyTransactionAmountsInEachChain'].items():
                data['dailyTransactionAmounts'][timestamp] = sum(value.values())

            self.local_klg.update_document("multichain_wallets", data)

    def export_klg_data(self):
        debtors = self.mongodb.get_documents("debtors", {})
        debt_addresses = []
        multichain_debt_addresses = []
        for debtor in debtors:
            for chain_id in ["0xfa", "0x38", "0x1", "0x89"]:
                debt_addresses.append(f"{chain_id}_{debtor['_id']}")
            multichain_debt_addresses.append(debtor['_id'])
        wallets = list(self.main_klg.get_wallets_by_keys(debt_addresses))
        for wallet in wallets:
            data = {
                "_id": wallet["_id"],
                "address": wallet["address"],
                "depositChangeLogs": wallet.get("depositChangeLogs") or {},
                "borrowChangeLogs": wallet.get("borrowChangeLogs") or {},
                "borrowTokens": wallet.get("borrowTokens") or {},
                "depositTokens": wallet.get("depositTokens") or {},
                "lendings": wallet.get("lendings") or {},
                "liquidationLogs": wallet.get("liquidationLogs") or {},
                "createdAt": wallet.get("createdAt") or int(time.time()),
                "numberOfLiquidation": wallet.get("numberOfLiquidation") or 0,
                "totalValueOfLiquidation": wallet.get("totalValueOfLiquidation") or 0,
                "depositInUSD": wallet.get("depositInUSD") or 0,
                "borrowInUSD": wallet.get("borrowInUSD") or 0,
                "depositTokenChangeLogs": wallet.get("depositTokenChangeLogs") or {},
                "borrowTokenChangeLogs": wallet.get("borrowTokenChangeLogs") or {}
            }
            self.local_klg.update_document("wallets", data)

        multichain_wallets = self.main_klg.get_multichain_wallets(multichain_debt_addresses)
        for wallet in multichain_wallets:
            data = {
                "_id": wallet.get("_id"),
                "address": wallet.get("address"),
                "depositChangeLogs": wallet.get("depositChangeLogs") or {},
                "borrowChangeLogs": wallet.get("borrowChangeLogs") or {},
                "borrowTokens": wallet.get("borrowTokens") or {},
                "depositTokens": wallet.get("depositTokens") or {},
                "lendings": wallet.get("lendings") or {},
                "liquidationLogs": wallet.get("liquidationLogs") or {},
                "createdAt": wallet.get("createdAt") or int(time.time()),
                "numberOfLiquidation": wallet.get("numberOfLiquidation") or 0,
                "totalValueOfLiquidation": wallet.get("totalValueOfLiquidation") or 0,
                "depositInUSD": wallet.get("depositInUSD") or 0,
                "borrowInUSD": wallet.get("borrowInUSD") or 0,
                "depositTokenChangeLogs": wallet.get("depositTokenChangeLogs") or {},
                "borrowTokenChangeLogs": wallet.get("borrowTokenChangeLogs") or {}
            }
            self.local_klg.update_document("multichain_wallets", data)

    def export_contracts(self):
        tokens = self.local_klg.get_document("configs", {"_id": "top_tokens_0x89"})
        token_list = []
        for token in tokens["tokens"]:
            token_list.append(f"0x89_{token['address']}")
        tokens = self.main_klg.get_contracts(keys=token_list)
        for token in tokens:
            self.local_klg.update_document("smart_contracts", token)
        # with open("token.json", 'w') as f:
        #     f.write(json.dumps(token_list, indent=1))

    def export_debtor_list(self):
        data = self.mongodb.get_documents("debtors", {})
        result = {}
        for wallet in data:
            result[wallet["_id"]] = True
        # result = []
        # for wallet in data:
        #     result.append(wallet["_id"])
        with open("debtor.json", "w") as f:
            json.dump(result, f, indent=1)

    def check_negative_balance(self):
        result = {}
        for item in self.local_klg.get_documents("multichain_wallets", {}):
            if "tokenChangeLogs" not in item: continue
            for token, value in item["tokenChangeLogs"].items():
                for timestamp, amount in value.items():
                    if amount["amount"] < 0:
                        if token not in result: result[token] = []
                        if item['address'] not in result[token]:
                            result[token].append(item['address'])
        with open("wrong_balance.json", "w") as f:
            json.dump(result, f, indent=1)

    def cluster_debtors(self, list_abnormal_timestamp, abnormal_wallets):
        min_wallets = []
        max_wallets = []
        normal_wallets = []
        abnormal_time_debtors = {str(timestamp): [] for timestamp in list_abnormal_timestamp}
        for event in self.mongodb.get_documents("lending_events", {}):
            check_wallet = False
            if event["debt_to_cover_in_usd"] > 20000:
                max_wallets.append(event["user"])
                check_wallet = True
            elif event["debt_to_cover_in_usd"] < 10:
                min_wallets.append(event["user"])
                check_wallet = True

            for timestamp in list_abnormal_timestamp:
                if round_timestamp(event["block_timestamp"]) == timestamp and \
                        event["user"] not in abnormal_time_debtors[str(timestamp)]:
                    abnormal_time_debtors[str(timestamp)].append(event["user"])
                    check_wallet = True
                    break

            if event["user"] not in abnormal_wallets and not check_wallet:
                normal_wallets.append(event["user"])
        data = {
            "_id": "wallet_cluster",
            "min_wallets": list(set(min_wallets)),
            "max_wallets": list(set(max_wallets)),
            "abnormal_time_debtors": abnormal_time_debtors,
            "normal_wallets": list(set(normal_wallets))
        }
        self.mongodb.update_document("configs", data)

    def export_abnormal_wallets(self):
        users = self.mongodb.get_document("configs", {"_id": "wallet_cluster"})
        one_liquidated_wallets, multiple_liquidated_wallets = self.get_debtors(users)
        wallets = self.local_klg.get_documents(
            "multichain_wallets_credit_scores", {"_id": {"$in": one_liquidated_wallets}})
        result = {"_id": "abnormal_wallets", "abnormal_one_liquidated_wallets": [],
                  "abnormal_multiple_liquidated_wallets": []}
        for wallet in wallets:
            for key in wallet:
                if key in ["_id", "address", "flagged"]:
                    continue
                start_time = int(key) - 7 * 24 * 3600
                if wallet[key][key]["creditScore"] > wallet[key][str(start_time)]["creditScore"] and \
                        wallet["_id"] not in result["abnormal_one_liquidated_wallets"]:
                    result["abnormal_one_liquidated_wallets"].append(wallet["_id"])

        wallets = self.local_klg.get_documents(
            "multichain_wallets_credit_scores", {"_id": {"$in": multiple_liquidated_wallets}})
        for wallet in wallets:
            for key in wallet:
                if key in ["_id", "address", "flagged"]:
                    continue
                start_time = int(key) - 7 * 24 * 3600
                if wallet[key][key]["creditScore"] > wallet[key][str(start_time)]["creditScore"] \
                        and wallet["_id"] not in result["abnormal_multiple_liquidated_wallets"]:
                    result["abnormal_multiple_liquidated_wallets"].append(wallet["_id"])
        self.local_klg.update_document("configs", result)

    def export_scoring_timestamp(self, end_time):
        cursor = self.local_klg.get_documents("debtors", {})
        for debt in cursor:
            address = debt["_id"]
            scoring_times = {}
            for buyer, value in debt["buyers"].items():
                for timestamp in value:
                    start_ = int(timestamp) - 7 * 24 * 3600
                    end_ = int(timestamp) + 7 * 24 * 3600
                    scoring_times[str(timestamp)] = [i for i in range(start_, end_ + 3600, 3600) if i < end_time]
            data = {
                "_id": address,
                "scoringTimestamps": scoring_times,
                "flagged": 1
            }
            self.local_klg.update_document("multichain_wallets", data)

    def get_debtors(self, users):
        one_liquidated_wallets = []
        multiple_liquidated_wallets = []
        for debtor in self.local_klg.get_documents("debtors", {}):
            if debtor["_id"] not in users["normal_wallets"]:
                continue
            count = debtor["count"]
            if count == 1:
                one_liquidated_wallets.append(debtor["_id"])
            else:
                multiple_liquidated_wallets.append(debtor["_id"])
        return one_liquidated_wallets, multiple_liquidated_wallets

    def check_number_of_liquidation(self, end_timestamp):
        users = self.mongodb.get_document("configs", {"_id": "wallet_cluster"})
        one_liquidated_wallets, _ = self.get_debtors(users)
        m = 0
        for wallet in self.local_klg.get_documents("multichain_wallets", {"_id": {"$in": one_liquidated_wallets}}):
            for key, value in wallet["liquidationLogs"]["liquidatedWallet"].items():
                for timestamp in value:
                    if int(timestamp) < end_timestamp:
                        m += 1
        print(m / len(one_liquidated_wallets))
        return m / len(one_liquidated_wallets)

    def export_credit_score_change(self):
        one_liquidated_wallets = []
        multiple_liquidated_wallets = []
        for debtor in self.local_klg.get_documents("debtors", {}, {"buyers": 1}):
            count = 0
            for wallet, value in debtor['buyers'].items():
                count += len(value.keys())
            data = {
                "_id": debtor["_id"],
                "count": count
            }
            if count == 1:
                one_liquidated_wallets.append(debtor["_id"])
            else:
                multiple_liquidated_wallets.append(debtor["_id"])
            self.local_klg.update_document("debtors", data)

        for debtor in self.local_klg.get_documents(
                "multichain_wallets_credit_scores", {"_id": {"$in": one_liquidated_wallets}}):
            data = {"_id": debtor["_id"]}
            for key in debtor:
                if key in ["_id", "address", "flagged"]: continue
                start_time = int(key) - 7 * 24 * 3600
                start_value = debtor[key][str(start_time)]["creditScore"]
                data[key] = {}
                for timestamp, value in debtor[key].items():
                    if str(start_time) == timestamp:
                        continue
                    data[key][timestamp] = start_value - value["creditScore"]
                    data["last_score_change"] = start_value - value["creditScore"]
            self.local_klg.update_document("debtors", data)

        for debtor in self.local_klg.get_documents(
                "multichain_wallets_credit_scores", {"_id": {"$in": multiple_liquidated_wallets}}):
            data = {"_id": debtor["_id"]}
            min_time = time.time()
            max_time = 0
            start_time, end_time = 0, 0
            start_value = 0
            time1, time2 = 0, 0
            data["liquidate_time"] = []
            for key in debtor:
                if key in ["_id", "address", "flagged"]: continue
                data["liquidate_time"].append(key)
                if int(key) < min_time:
                    start_time = int(key) - 7 * 24 * 3600
                    start_value = debtor[key][str(start_time)]["creditScore"]
                    time1 = key
                    min_time = int(key)
                if int(key) > max_time:
                    time2 = key
                    max_time = int(key)

            for key in debtor:
                if key in ["_id", "address", "flagged"]:
                    continue
                if key == time1:
                    data[key] = {}
                    for timestamp, value in debtor[key].items():
                        if str(start_time) == timestamp:
                            continue
                        data[key][timestamp] = start_value - value["creditScore"]
                else:
                    data[key] = {}
                    for timestamp, value in debtor[key].items():
                        data[key][timestamp] = start_value - value["creditScore"]
                        data["last_score_change"] = start_value - value["creditScore"]
            data["liquidate_time"].sort()
            self.local_klg.update_document("debtors", data)

    def export_debt_amount(self, chain_id):
        datetime_list = []
        timestamps = []
        for year in [2020, 2021, 2022, 2023]:
            for number in range(1, 13):
                if year == 2020 and number < 10: continue
                element = datetime.datetime(year, number, 1, 0, 0, tzinfo=datetime.timezone.utc)
                datetime_list.append(element)
                timestamps.append(datetime.datetime.timestamp(element))
        timestamps.sort()
        cursors = self.mongodb.get_documents('debtors', {})
        result, df = self.check_chain_id(chain_id, timestamps)
        for wallet in cursors:
            for key, value in wallet["buyers"].items():
                for timestamp, liquidate in value.items():
                    for _timestamp in timestamps:
                        if int(timestamp) < int(_timestamp):
                            if liquidate.get("protocolName") in ["trava", "valas", "aave", "geist"]:
                                result[_timestamp]["aave_folks"] += liquidate.get("debtAssetInUSD")
                            else:
                                result[_timestamp]["compound_folks"] += liquidate.get("debtAssetInUSD")
                            result[_timestamp][liquidate.get("protocolName")] += liquidate.get("debtAssetInUSD")
                            result[_timestamp]["total"] += liquidate.get("debtAssetInUSD")
                            break
        for item in result:
            df["timestamps"].append(datetime.datetime.utcfromtimestamp(item))
            for key in df:
                if key == "timestamps": continue
                df[key].append(result[item][key])
        df = pd.DataFrame(df)
        df.to_csv(f"{chain_id}_liquidated_debt_asset.csv")
        with open(f"{chain_id}_debt_asset_liquidation.json", "w") as f:
            f.write(json.dumps(result, indent=1))

    @staticmethod
    def check_chain_id(chain_id, timestamps):
        df = {}
        result = {}
        if chain_id == "0x38":
            result = {key: {"trava": 0, "valas": 0, "cream": 0, "venus": 0,
                            "aave_folks": 0, "compound_folks": 0, "total": 0} for key in timestamps}
            df = {"timestamps": [], "trava": [], "valas": [], "cream": [],
                  "venus": [], "aave_folks": [], "compound_folks": [], "total": []}
        if chain_id == "0x1":
            result = {key: {"trava": 0, "aave": 0, "compound": 0, "aave_folks": 0,
                            "compound_folks": 0, "total": 0} for key in timestamps}
            df = {"timestamps": [], "trava": [], "aave": [], "compound": [],
                  "aave_folks": [], "compound_folks": [], "total": []}

        if chain_id == "0xfa":
            result = {key: {"trava": 0, "geist": 0, "aave_folks": 0, "compound_folks": 0, "total": 0}
                      for key in timestamps}
            df = {"timestamps": [], "trava": [], "geist": [], "aave_folks": [], "compound_folks": [], "total": []}

        return result, df
