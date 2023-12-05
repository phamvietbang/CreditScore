import json

from constants.constants import Chain
from database.mongodb import MongoDB
import pandas as pd

debtors = []
for key in Chain.mapping:
    mongo = MongoDB("mongodb://localhost:27017/", database="blockchain_etl", db_prefix=key)
    for debtor in mongo.get_documents("debtors", {}):
        if debtor.get("_id") not in debtors:
            debtors.append(debtor.get("_id"))

# with open("data/debtors.json", "w") as f:
#     json.dump(debtors, f, indent=1)

with open("centralized_exchange_addresses.json", "r") as f:
    data = json.loads(f.read())
result = {"wallet":[], "chain":[], "name":[]}
for key, value in data.items():
    name = value.get("name")
    for chain, list_add in value.get("wallets").items():
        for add in list_add:
            result['wallet'].append(add)
            result["chain"].append(chain)
            result["name"].append(name)
result = pd.DataFrame(result)
result.to_csv("centralized_exchange_addresses.csv")
debtors = pd.DataFrame({"wallet":debtors})
debtors.to_csv("liquidated_wallets.csv")