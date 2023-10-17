import json

import pandas as pd
from matplotlib import pyplot as plt

from database.mongodb import MongoDB


class HistogramDrawer:
    def __init__(self, klg_mongodb: MongoDB):
        self.klg_mongodb = klg_mongodb

    def draw_histogram_one_time_liquidated_wallets(self):
        wallets = self.klg_mongodb.get_documents("multichain_wallets_credit_scores",
                                                 {"count": 1})
        score = {}

        for i in range(1, 16):
            score[f"t{i}"] = {"<580": 0, "580-669": 0, "670-739": 0, "740-799": 0, "800-850": 0}
        for wallet in wallets:
            for key in wallet:
                if key in ["_id", "address", "flagged", "minTime", "count", "maxTime"]:
                    continue
                scores = dict(sorted(wallet[key].items(), key=lambda x: x[0]))
                list_time = [key, str(int(key) - 3600), str(int(key) + 3600)]
                for tmp in range(1, 7):
                    list_time += [
                        str(int(key) - 3600 * 24 * tmp),
                        str(int(key) + 3600 * 24 * tmp)
                    ]
                list_time.sort()
                keys = list(scores.keys())
                keys.sort()
                pos = 1
                for time_ in list_time:
                    if time_ not in keys:
                        score[f"t{pos}"] = score[f"t{pos - 1}"]
                        continue
                    value = scores[time_]["creditScore"]
                    self.check_value(value, score, pos)
                    pos += 1

        _, axs = plt.subplots(1, 2, figsize=(10, 5))
        axs[0].bar(score["t1"].keys(), score["t1"].values())
        axs[1].bar(score["t8"].keys(), score["t8"].values())
        axs[0].set_ylabel("Number of Wallets")
        axs[0].set_xlabel("Score range")
        axs[1].set_xlabel("Score range")
        axs[0].set_title("-6 days")
        axs[1].set_title("liquidation")
        plt.savefig("hist1.png")

    def draw_histogram_multiple_times_liquidated_wallets(self):

        wallets = self.klg_mongodb.get_documents(
            "multichain_wallets_credit_scores", {"count":{"$gt":1}})
        score = {}
        for i in range(1, 17):
            score[f"t{i}"] = {"<580": 0, "580-669": 0, "670-739": 0, "740-799": 0, "800-850": 0}
        for wallet in wallets:
            start_lq_time = wallet['minTime']
            end_lq_time = wallet['maxTime']
            score_start = dict(sorted(wallet[str(start_lq_time)].items(), key=lambda x: x[0]))
            score_end = dict(sorted(wallet[str(end_lq_time)].items(), key=lambda x: x[0]))
            score_start.update(score_end)
            list_time = [str(start_lq_time), str(end_lq_time),
                         str(int(end_lq_time) + 3600),
                         str(int(start_lq_time) - 3600)]
            for tmp in range(1, 7):
                list_time += [
                    str(int(start_lq_time) - 3600 * 24 * tmp),
                    str(int(end_lq_time) + 3600 * 24 * tmp)
                ]
            list_time.sort()
            keys = list(score_start.keys())
            keys.sort()
            pos = 1
            for time_ in list_time:
                if time_ not in keys:
                    score[f"t{pos}"] = score[f"t{pos - 1}"]
                    continue
                value = score_start[time_]["creditScore"]
                self.check_value(value, score, pos)
                pos += 1

        _, axs = plt.subplots(1, 3, figsize=(15, 5))
        axs[0].bar(score["t1"].keys(), score["t1"].values())
        axs[1].bar(score["t8"].keys(), score["t8"].values())
        axs[2].bar(score["t9"].keys(), score["t9"].values())
        axs[0].set_ylabel("Number of Wallets")
        axs[0].set_xlabel("Score range")
        axs[1].set_xlabel("Score range")
        axs[2].set_xlabel("Score range")
        axs[0].set_title("-6 days")
        axs[1].set_title("liquidation 1")
        axs[2].set_title("liquidation 2")
        plt.savefig("hist2.png")

    @staticmethod
    def check_value(value, score, pos):
        if value < 580:
            score[f"t{pos}"]["<580"] += 1
        elif 580 <= value < 670:
            score[f"t{pos}"]["580-669"] += 1
        elif 670 <= value < 740:
            score[f"t{pos}"]["670-739"] += 1
        elif 740 <= value < 800:
            score[f"t{pos}"]["740-799"] += 1
        elif 800 <= value < 850:
            score[f"t{pos}"]["800-850"] += 1

    def get_debtor_info(self, users, abnormal_wallets):
        one_liquidated_wallets = []
        multiple_liquidated_wallets = []
        liquidate_time = {}
        for debtor in self.klg_mongodb.get_documents("debtors", {}):
            if debtor["_id"] not in users.get("normal_wallets") or debtor["_id"] in abnormal_wallets: continue
            count = debtor["count"]
            if count == 1:
                one_liquidated_wallets.append(debtor["_id"])
            else:
                multiple_liquidated_wallets.append(debtor["_id"])
                liquidate_time[debtor['_id']] = debtor["liquidate_time"]

        return one_liquidated_wallets, multiple_liquidated_wallets, liquidate_time

if __name__ == "__main__":
    klg_mongo = MongoDB("mongodb://localhost:27017/", database="knowledge_graph")
    with open('user.json', 'r') as f:
        elite_wallets = json.loads(f.read())
    job = HistogramDrawer(klg_mongo)
    job.draw_histogram_one_time_liquidated_wallets()
    # job.draw_histogram_multiple_times_liquidated_wallets()