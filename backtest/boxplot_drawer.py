import json
import time
from os import times

import pandas as pd
from matplotlib import pyplot as plt

from database.mongodb import MongoDB


class BoxPlotDrawer:
    def __init__(self, klg_mongodb: MongoDB):
        self.klg_mongodb = klg_mongodb

    def draw_boxplot_one_time_liquidated_wallets(self, liquidation_timestamp):
        score = {}
        list_time_labels = [
            "-6days", "-5days", "-4days",
            "-3days", "-2days", "-1day",
            "-1hour", "liquidation", "1hour",
            "1day", "2days", "3days",
            "4days", "5days", "6days"]
        wallets = self.klg_mongodb.get_documents("multichain_wallets_credit_scores", {"count": 1})
        for wallet in wallets:
            liquidated_time = liquidation_timestamp.get(wallet.get('_id'))
            score_change = dict(sorted(wallet.get("scores").items(), key=lambda x: x[0]))
            list_time = [liquidated_time, str(int(liquidated_time) - 3600), str(int(liquidated_time) + 3600)]
            for tmp in range(1, 7):
                list_time += [
                    str(int(liquidated_time) - 3600 * 24 * tmp),
                    str(int(liquidated_time) + 3600 * 24 * tmp),
                ]
            list_time.sort()
            keys = list(score_change.keys())
            keys.sort()
            start_score = score_change.get(str(int(liquidated_time) - 3600 * 24 * 7)).get('creditScore')
            if not start_score:
                start_score = score_change.get(keys[0]).get('creditScore')
            pos = 0
            for time_ in list_time:
                if list_time_labels[pos] not in score:
                    score[list_time_labels[pos]] = []
                if time_ not in keys:
                    tmp_score = 0
                    for key in keys:
                        tmp_score = start_score - score_change[key]['creditScore']
                        if int(key) > int(time_):
                            break
                    score[list_time_labels[pos]].append(tmp_score)
                else:
                    score[list_time_labels[pos]].append(start_score - score_change[time_]['creditScore'])
                pos += 1
        m = [i for i in score[list_time_labels[7]] if i > 0]
        print("Average score in liquidation: ", sum(m) / len(m))
        plt.figure(figsize=(20, 10))
        df = pd.DataFrame(score, columns=[pos for pos in score.keys()])
        ax = df.boxplot(column=[pos for pos in score.keys()],figsize=(20, 10), fontsize=14)
        plt.xticks(rotation=45)
        ax.set_ylabel("Score change",fontsize=14)
        # ax.set_xlabel("Milestone")
        # ax.set_title("Score change boxplot")
        plt.savefig("liquidate1.png")

    def get_debtors(self):
        one_liquidated_wallets = []
        multiple_liquidated_wallets = []
        for debtor in self.klg_mongodb.get_documents("debtors", {}):
            count = debtor["count"]
            if count == 1:
                one_liquidated_wallets.append(debtor["_id"])
            else:
                multiple_liquidated_wallets.append(debtor["_id"])
        return one_liquidated_wallets, multiple_liquidated_wallets

    def draw_boxplot_multiple_times_liquidated_wallets(self,  liquidation_timestamps):
        score = {}
        _, multiple_liquidated_wallets = self.get_debtors()
        list_time_labels = ["-6days", "-5days", "-4days", "-3days", "-2days", "-1day",
                            "-1hour", "liquidation1", "liquidation2", "1hour",
                            "1day", "2days", "3days", "4days", "5days", "6days"]
        wallets = self.klg_mongodb.get_documents("multichain_wallets_credit_scores", {"count": {'$gt': 1}})
        for wallet in wallets:
            start_lq_time = liquidation_timestamps.get(wallet.get("_id"))["minTime"]
            end_lq_time = liquidation_timestamps.get(wallet.get("_id"))["maxTime"]
            score_change = dict(sorted(wallet.get("scores").items(), key=lambda x: x[0]))
            list_time = [str(start_lq_time), str(end_lq_time),
                         str(int(end_lq_time) + 3600),
                         str(int(start_lq_time) - 3600)]

            for tmp in range(1, 7):
                list_time += [
                    str(int(start_lq_time) - 3600 * 24 * tmp),
                    str(int(end_lq_time) + 3600 * 24 * tmp),
                ]
            list_time.sort()
            keys = list(score_change.keys())
            keys.sort()
            start_score = score_change.get(keys[0]).get('creditScore')
            pos = 0
            for time_ in list_time:
                if list_time_labels[pos] not in score:
                    score[list_time_labels[pos]] = []
                if time_ not in keys:
                    tmp_score = 0
                    for key in keys:
                        tmp_score = start_score - score_change[key]['creditScore']
                        if int(key) > int(time_):
                            break
                    score[list_time_labels[pos]].append(tmp_score)
                else:
                    score[list_time_labels[pos]].append(start_score - score_change[time_]['creditScore'])
                pos += 1
        m = [i for i in score[list_time_labels[7]] if i > 0]
        print("Average score in 1st liquidation: ", sum(m) / len(m))
        m = [i for i in score[list_time_labels[8]] if i > 0]
        print("Average score in the last liquidation: ", sum(m) / len(m))
        df = pd.DataFrame(score, columns=[pos for pos in score.keys()])
        ax = df.boxplot(column=[pos for pos in score.keys()], figsize=(20, 10), fontsize=14)
        ax.set_ylabel("Score change", fontsize=14)
        # ax.set_xlabel("Milestone")
        # ax.set_title("Score change boxplot")
        plt.xticks(rotation=45)
        plt.savefig("liquidate2.png")


if __name__ == "__main__":
    klg_mongo = MongoDB("mongodb://localhost:27017/", database="knowledge_graph")
    klg_mongo_main = MongoDB(
        "mongodb://klgWriter:klgEntity_writer523@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017/",
        database="knowledge_graph")
    wallets = [wallet.get("_id") for wallet in klg_mongo.get_documents("multichain_wallets_credit_scores", {"count": 1})]
    # with open("one_times_wallets.json", 'r') as f:
    #     wallets = json.loads(f.read())
    liquidation_timestamp = {}
    for wallet in klg_mongo.get_documents("multichain_wallets", {"_id": {"$in": wallets}}):
        for key, value in wallet.get("liquidationLogs", {}).get("liquidatedWallet").items():
            for timestamp in value.keys():
                if 1719792000 <= int(timestamp) < 1727740800:
                    liquidation_timestamp[wallet.get("_id")] = timestamp
                    break
    # # for item in klg_mongo_main.get_documents("wallets", {"address":{"$in": wallets}}):
    # #     if item.get("project"):
    # #         wallets.pop(item.get("address"))
    # # with open("one_times_wallets_timestamp.json", 'r') as f:
    # #     liquidation_timestamp = json.loads(f.read())
    job = BoxPlotDrawer(klg_mongo)
    # job.draw_boxplot_one_time_liquidated_wallets(liquidation_timestamp)
    # job.draw_boxplot_multiple_times_liquidated_wallets()

    multi_wallet_timestamps = {}
    for wallet in klg_mongo.get_documents("multichain_wallets", {"_id": {"$nin": wallets}}):
        min_, max_ = time.time(), 0
        for key, value in wallet.get("liquidationLogs", {}).get("liquidatedWallet").items():
            for timestamp in value.keys():
                if 1719792000 <= int(timestamp) < 1727740800:
                    if int(timestamp) < min_:
                        min_ = int(timestamp)
                    if int(timestamp) > max_:
                        max_ = int(timestamp)
        multi_wallet_timestamps[wallet.get("_id")] = {
            "minTime": str(min_),
            "maxTime": str(max_)
        }

    # with open("multiple_times_wallets_timestamp.json", 'r') as f:
    #     multi_wallet_timestamps = json.loads(f.read())
        # json.dump(multi_wallet_timestamps, f, indent=1)
    #
    job.draw_boxplot_multiple_times_liquidated_wallets(multi_wallet_timestamps)