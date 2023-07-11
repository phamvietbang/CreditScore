import pandas as pd
from matplotlib import pyplot as plt

from database.mongodb import MongoDB


class BoxPlotDrawer:
    def __init__(self, mongodb: MongoDB, klg_mongodb: MongoDB):
        self.mongodb = mongodb
        self.klg_mongodb = klg_mongodb

    def draw_boxplot_one_time_liquidated_wallets(self):
        score = {}
        one_liquidated_wallets, _ = self.get_debtors()
        list_time_labels = ["-6days", "-5days", "-4days", "-3days", "-2days", "-1day",
                            "-1hour", "liquidation", "1hour",
                            "1day", "2days", "3days", "4days", "5days", "6days"]
        users = self.mongodb.get_document("configs", {"_id": "wallet_cluster"})
        wallets = self.klg_mongodb.get_documents("debtors", {"_id": {"$in": one_liquidated_wallets}})
        for wallet in wallets:
            if wallet["_id"] not in users.get("normal_wallets"):
                continue
            for key in wallet:
                if key in ["_id", "address", "flagged", "buyers", "count", "last_score_change"]:
                    continue
                score_change = dict(sorted(wallet[key].items(), key=lambda x: x[0]))
                list_time = [key, str(int(key) - 3600), str(int(key) + 3600)]
                for tmp in range(1, 7):
                    list_time += [
                        str(int(key) - 3600 * 24 * tmp),
                        str(int(key) + 3600 * 24 * tmp),
                    ]
                list_time.sort()
                keys = list(score_change.keys())
                keys.sort()
                pos = 0
                for time_ in list_time:
                    if list_time_labels[pos] not in score:
                        score[list_time_labels[pos]] = []
                    if time_ not in keys:
                        score[list_time_labels[pos]].append(score_change[keys[-1]])
                    else:
                        score[list_time_labels[pos]].append(score_change[time_])
                    pos += 1
        m = [i for i in score[list_time_labels[7]] if i > 0]
        print("Average score in liquidation: ", sum(m) / len(m))
        df = pd.DataFrame(score, columns=[pos for pos in score.keys()])
        ax = df.boxplot(column=[pos for pos in score.keys()])
        plt.xticks(rotation=45)
        ax.set_ylabel("Score change")
        ax.set_xlabel("Milestone")
        ax.set_title("Score change boxplot")
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

    def draw_boxplot_multiple_times_liquidated_wallets(self):
        score = {}
        _, multiple_liquidated_wallets = self.get_debtors()
        list_time_labels = ["-6days", "-5days", "-4days", "-3days", "-2days", "-1day",
                            "-1hour", "liquidation1", "liquidation2", "1hour",
                            "1day", "2days", "3days", "4days", "5days", "6days"]
        wallets = self.klg_mongodb.get_documents("debtors", {"_id": {"$in": multiple_liquidated_wallets}})
        users = self.mongodb.get_document("configs", {"_id": "wallet_cluster"})
        for wallet in wallets:
            if wallet["_id"] in users.get("normal_wallets"):
                continue
            start_lq_time = wallet["liquidate_time"][0]
            end_lq_time = wallet["liquidate_time"][-1]
            score_change_start = dict(sorted(wallet[start_lq_time].items(), key=lambda x: x[0]))
            score_change_end = dict(sorted(wallet[end_lq_time].items(), key=lambda x: x[0]))
            score_change_start.update(score_change_end)
            list_time = [start_lq_time, end_lq_time,
                         str(int(end_lq_time) + 3600),
                         str(int(start_lq_time) - 3600)]

            for tmp in range(1, 7):
                list_time += [
                    str(int(start_lq_time) - 3600 * 24 * tmp),
                    str(int(end_lq_time) + 3600 * 24 * tmp),
                ]
            list_time.sort()
            keys = list(score_change_start.keys())
            keys.sort()
            pos = 0
            for time_ in list_time:
                if list_time_labels[pos] not in score:
                    score[list_time_labels[pos]] = []
                if time_ not in keys:
                    score[list_time_labels[pos]].append(score_change_start[keys[-1]])
                else:
                    score[list_time_labels[pos]].append(score_change_start[time_])
                pos += 1
        m = [i for i in score[list_time_labels[7]] if i > 0]
        print("Average score in 1st liquidation: ", sum(m) / len(m))
        m = [i for i in score[list_time_labels[8]] if i > 0]
        print("Average score in the last liquidation: ", sum(m) / len(m))
        df = pd.DataFrame(score, columns=[pos for pos in score.keys()])
        ax = df.boxplot(column=[pos for pos in score.keys()])
        ax.set_ylabel("Score change")
        ax.set_xlabel("Milestone")
        ax.set_title("Score change boxplot")
        plt.xticks(rotation=45)
        plt.show()
