import json

from constants.constants import Chain
from database.mongodb import MongoDB

debtors = []
for key in Chain.mapping:
    mongo = MongoDB("mongodb://localhost:27017/", database="blockchain_etl", db_prefix=key)
    for debtor in mongo.get_documents("debtors", {}):
        if debtor.get("_id") not in debtors:
            debtors.append(debtor.get("_id"))

with open("data/debtors.json", "w") as f:
    json.dump(debtors, f, indent=1)