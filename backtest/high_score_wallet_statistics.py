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
score = {"x1": [], "x2": [], "x3": [], "x4": [], "x5": [], "x6": [], "x7": []}
one_liquidated_wallets = []
multiple_liquidated_wallets = []
users = mongodb.get_document("configs", {"_id": "wallet_cluster"})
all = abnormal_wallets["abnormal_one_liquidated_wallets"] + abnormal_wallets[
        "abnormal_multiple_liquidated_wallets"]
for debtor in klg_mongodb.get_documents("debtors", {}):
    if debtor["_id"] not in users.get("normal_wallets"): continue
    if debtor["_id"] in all: continue
    count = debtor["count"]
    if count == 1:
        one_liquidated_wallets.append(debtor["_id"])
    else:
        multiple_liquidated_wallets.append(debtor["_id"])


for wallet in data:
    if wallet["_id"] not in multiple_liquidated_wallets: continue
    for key in wallet:
        if key in ["_id", "address", "flagged"]:
            continue
        start_time = str(int(key) - 7 * 24 * 3600)
        _hour = str(int(key) - 24 * 3600)
        if wallet[key][key]["creditScore"] >= 600:
            scores1 = cal_params(wallet[key][key])
            for i in range(len(scores1)):
                score[f"x{i+1}"].append(scores1[i])
            print(wallet["address"])

fig, axs = plt.subplots(nrows=1, ncols=1, figsize=(10, 10))
for i in score:
    if not score[i]:
        score[i] = 0
        continue
    score[i] = sum(score[i])/len(score[i])
axs.set_title('')
axs.bar(score.keys(), score.values())
plt.savefig("params1.png")
