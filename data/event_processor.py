import json

import pandas as pd
from web3 import Web3, HTTPProvider

from abi.erc20_abi import ERC20_ABI
from database.mongodb import MongoDB
from database.postgre_sql import TransferPostgresqlStreamingExporter
from utils.time_utils import round_timestamp


class EventProcessor:
    def __init__(self, provider, mongodb: MongoDB, main_mongo: MongoDB, klg_mongodb: MongoDB,
                 postgres: TransferPostgresqlStreamingExporter):
        self.main_mongo = main_mongo
        self.mongodb = mongodb
        self.w3 = Web3(HTTPProvider(provider))
        self.klg = klg_mongodb
        self.postgres = postgres

    def export_evt_tf(self, file, chain_id):
        data = pd.read_csv(file)
        result = []
        token_decimals = []
        dict_token_decimals = {}
        tokens = self.klg.get_document("configs", {"_id": f"top_tokens_{chain_id}"})
        token_list = []
        for token in tokens["tokens"]:
            token_list.append(token["address"])
        for pos in range(len(data['evt_block_number'])):
            token = data["contract_address"][pos]
            # if token not in token_list:
            #     continue
            if token not in token_list:
                dict_token_decimals[token] = 18
            if token not in dict_token_decimals:
                decimals = list(self.postgres.get_decimals([token]))
                if not decimals:
                    contract = self.w3.eth.contract(address=self.w3.toChecksumAddress(token), abi=ERC20_ABI)
                    decimals = contract.functions.decimals().call()
                    token_decimals.append({
                        "address": token,
                        "decimals": decimals
                    })

                else:
                    decimals = decimals[0][1]
                dict_token_decimals[token] = decimals
            result.append({
                "contract_address": data["contract_address"][pos],
                "transaction_hash": data["evt_tx_hash"][pos],
                "from_address": data["from"][pos],
                "to_address": data["to"][pos],
                "value": float(data["value"][pos]) / 10 ** dict_token_decimals[token],
                "log_index": int(data["evt_index"][pos]),
                "block_number": int(data["evt_block_number"][pos])
            })
        if token_decimals:
            self.postgres.export_token_decimals(token_decimals)
        self.postgres.export_items(result)

    def liquidate_event_analysis(self, start_time, end_time):
        events = list(self.mongodb.get_documents("lending_events", {}))
        users_df = {"wallet": [], "number_of_liquidation": [], "debt_amount": []}
        liquidation_df = {"timestamp": [], "number_of_liquidation": [], "debt_amount": [], "number_of_debtors": []}
        users = {}
        liquidation = {}
        normal_users = self.mongodb.get_document("configs", {"_id": "wallet_cluster"})
        for timestamp in range(start_time, end_time, 86400):
            liquidation[timestamp] = {
                "number_of_liquidation": 0,
                "debt_amount": 0,
                "number_of_debtors": 0,
            }
        m = []
        check_debtor = {}
        for event in events:
            timestamp_ = round_timestamp(event["block_timestamp"])
            if timestamp_ not in check_debtor:
                check_debtor[timestamp_] = []
            # if timestamp_ in [1686355200, 1686700800]:
            #     continue
            debtor = event["user"]
            if debtor not in normal_users["normal_wallets"]: continue
            if debtor not in users:
                users[debtor] = {"number_of_liquidation": 0, "debt_amount": 0}
            users[debtor]["number_of_liquidation"] += 1
            users[debtor]["debt_amount"] += event["debt_to_cover_in_usd"]
            liquidation[timestamp_]["number_of_liquidation"] += 1
            liquidation[timestamp_]["debt_amount"] += event["debt_to_cover_in_usd"]
            if debtor not in check_debtor[timestamp_]:
                liquidation[timestamp_]["number_of_debtors"] += 1
                check_debtor[timestamp_].append(debtor)
            m.append(event["debt_to_cover_in_usd"])

        for key, value in liquidation.items():
            liquidation_df["timestamp"].append(key)
            liquidation_df["number_of_liquidation"].append(value["number_of_liquidation"])
            liquidation_df["debt_amount"].append(value["debt_amount"])
            liquidation_df["number_of_debtors"].append(value["number_of_debtors"])

        for key, value in users.items():
            users_df["wallet"].append(key)
            users_df["number_of_liquidation"].append(value["number_of_liquidation"])
            users_df["debt_amount"].append(value["debt_amount"])

        users_df = pd.DataFrame(users_df)
        liquidation_df = pd.DataFrame(liquidation_df)
        print("Information of Debtors\n", users_df.describe())
        print("Information of Liquidation\n", liquidation_df.describe())
        print(pd.DataFrame({"liquidate_event": m}).describe())
        print(liquidation_df[liquidation_df["number_of_liquidation"] >= 20])

    def check_liquidate_tokens(self, start_time, end_time):
        liquidated_events = self.mongodb.get_documents(
            "lending_events", {"block_timestamp": {"$gte": start_time, "$lte": end_time}})
        debt_tokens = {}
        collateral_tokens = {}
        for liquidated_event in liquidated_events:
            debt_token = liquidated_event["debt_asset"]
            collateral_token = liquidated_event["collateral_asset"]
            if debt_token not in debt_tokens:
                debt_tokens[debt_token] = {"number": 0, "amount": 0}
            if collateral_token not in collateral_tokens:
                collateral_tokens[collateral_token] = {"number": 0, "amount": 0}

            debt_tokens[debt_token]["number"] += 1
            collateral_tokens[collateral_token]["number"] += 1
            debt_tokens[debt_token]["amount"] += liquidated_event['debt_to_cover_in_usd']
            collateral_tokens[collateral_token]["amount"] += liquidated_event['liquidated_collateral_amount_in_usd']

        sorted_by_number = dict(sorted(debt_tokens.items(), key=lambda x: x[1]["number"], reverse=1))
        sorted_by_amount = dict(sorted(debt_tokens.items(), key=lambda x: x[1]["amount"], reverse=1))
        sorted_by_number_1 = dict(sorted(collateral_tokens.items(), key=lambda x: x[1]["number"], reverse=1))
        sorted_by_amount_1 = dict(sorted(collateral_tokens.items(), key=lambda x: x[1]["amount"], reverse=1))
        with open("debt_tokens.json", "w") as f:
            json.dump(sorted_by_number, f, indent=1)

        with open("debt_tokens2.json", "w") as f:
            json.dump(sorted_by_amount, f, indent=1)

        with open("collateral_tokens.json", "w") as f:
            json.dump(sorted_by_number_1, f, indent=1)

        with open("collateral_tokens2.json", "w") as f:
            json.dump(sorted_by_amount_1, f, indent=1)

    def export_tx(self, tx_mongodb: MongoDB, main_mongodb: MongoDB, start_block, end_block, range_block):
        # cursor = self.mongodb.get_documents("debtors", {})
        # wallets = []
        # for wallet in cursor:
        #     wallets.append(wallet["_id"])
        with open("debtors.json", 'r') as f:
            wallets = json.loads(f.read())
        for idx in range(start_block, end_block, range_block):
            cursor = main_mongodb.get_documents(
                "transactions",
                {"from_address": {"$in": wallets}, "block_number": {"$gte": idx, "$lte": idx + range_block}})
            for data in cursor:
                tx_mongodb.update_document("transactions", data)
            print(f"Exported from {idx} to {idx + range_block}")


if __name__ == "__main__":
    mongodb = MongoDB("mongodb://localhost:27017/", database="blockchain_etl", db_prefix="polygon")
    main_mongo = MongoDB(
        "mongodb://etlReader:etl_reader_tsKNV6KFr2GWqqqZ@34.126.84.83:27017,34.142.204.61:27017,34.142.219.60:27017/",
        database="blockchain_etl", db_prefix="polygon")
    klg_mongodb = MongoDB(
        connection_url="mongodb://klgWriter:klgEntity_writer523@35.198.222.97:27017,34.124.133.164:27017,34.124.205.24:27017/",
        database="knowledge_graph")
    postgres = TransferPostgresqlStreamingExporter(connection_url="postgresql://postgres:1369@localhost:5432/postgres")
    job = EventProcessor(
        provider="https://rpc.ankr.com/polygon",
        mongodb=mongodb,
        main_mongo=main_mongo,
        klg_mongodb=klg_mongodb,
        postgres=postgres
    )
    # job.export_evt_tf("./tfevent/polygon.csv", "0x89")
    # job.export_evt_tf("./tfevent/polygon2.csv", "0x89")
    # job.export_tx(mongodb, main_mongo, 17382265, 17595509, 100000)
