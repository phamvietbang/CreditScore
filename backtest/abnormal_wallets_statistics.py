from matplotlib import pyplot as plt

from calculate.services.statistic_service import about
from constants.constants import WalletCreditScoreWeightConstantV3
from database.mongodb import MongoDB


def cal_params(data):
    x51, x52 = data["creditScorex5"][0], data["creditScorex5"][1]
    x11, x12 = data["creditScorex1"][0], data["creditScorex1"][1]
    x21, x22 = data["creditScorex2"][0], data["creditScorex2"][1]
    x31, x32 = data["creditScorex3"][0], data["creditScorex3"][1]
    x41, x42, x43 = data["creditScorex4"][0], data["creditScorex4"][1], data["creditScorex4"][2]
    x61 = data["creditScorex6"][0]
    x71, x72 = data["creditScorex7"][0], data["creditScorex7"][1]
    x5 = about(x51 - (850 - x52))
    x6 = WalletCreditScoreWeightConstantV3.b61 * x61
    x1 = WalletCreditScoreWeightConstantV3.b11 * x11 + WalletCreditScoreWeightConstantV3.b12 * x12
    x2 = WalletCreditScoreWeightConstantV3.b21 * x21 + WalletCreditScoreWeightConstantV3.b22 * x22 + \
         WalletCreditScoreWeightConstantV3.b23 * 690 + WalletCreditScoreWeightConstantV3.b24 * 690
    x3 = WalletCreditScoreWeightConstantV3.b31 * x31 + WalletCreditScoreWeightConstantV3.b32 * x32
    x4 = WalletCreditScoreWeightConstantV3.b41 * x41 + WalletCreditScoreWeightConstantV3.b42 * x42 + \
         WalletCreditScoreWeightConstantV3.b43 * x43
    x7 = 0.85 * WalletCreditScoreWeightConstantV3.b71 * x71 + WalletCreditScoreWeightConstantV3.b72 * x72
    return x1, x2, x3, x4, x5, x6, x7


mongodb = MongoDB("mongodb://localhost:27017/", "blockchain_etl", db_prefix="ethereum")
klg_mongodb = MongoDB(connection_url="mongodb://localhost:27017/", database="knowledge_graph")
abnormal_wallets = klg_mongodb.get_document("configs", "abnormal_wallets")
data = klg_mongodb.get_documents("multichain_wallets_credit_scores", {})
boxplot_1_week = {"x1": [], "x2": [], "x3": [], "x4": [], "x5": [], "x6": [], "x7": []}
boxplot_1_hour = {"x1": [], "x2": [], "x3": [], "x4": [], "x5": [], "x6": [], "x7": []}
for wallet in data:
    for key in wallet:
        if key in ["_id", "address", "flagged", "count","maxTime", "minTime"]:
            continue
        start_time = str(int(key) - 7 * 24 * 3600)
        _hour = str(int(key) - 24 * 3600)
        if wallet[key][key]["creditScore"] > wallet[key][str(start_time)]["creditScore"]:
            scores1 = cal_params(wallet[key][key])
            scores2 = cal_params(wallet[key][start_time])
            scores3 = cal_params(wallet[key][_hour])
            for pos in range(len(scores1)):
                boxplot_1_week[f"x{pos + 1}"].append(
                    scores2[pos] - scores1[pos]
                )
                boxplot_1_hour[f"x{pos + 1}"].append(
                    scores3[pos] - scores1[pos]
                )
print(sum(boxplot_1_week["x3"]) / len(boxplot_1_week["x3"]))
print(sum(boxplot_1_week["x2"]) / len(boxplot_1_week["x2"]))
print(sum(boxplot_1_week["x4"]) / len(boxplot_1_week["x4"]))
fig, axs = plt.subplots(nrows=1, ncols=1, )

axs.set_title('Score change from 1 week ago to liquidation')
axs.boxplot(boxplot_1_week.values(), labels=[i for i in boxplot_1_week.keys()])
# axs[1].set_title('Score change in from 1 day ago to liquidation')
# axs[1].boxplot(boxplot_1_hour.values(), labels = [i for i in boxplot_1_hour.keys()])
plt.savefig("params1.png")
