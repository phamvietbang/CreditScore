import json

from database.cs_mongodb_klg import MongoDB as KLG
from database.mongodb import MongoDB


def calculate_all_chain(file):
    borrows = {}
    number_of_borrows = {}
    amount_of_borrows = {}
    n_borrows = {}
    for chain_id in ['0x38', '0xfa', '0x1', '0x89', '0xa86a', '0xa4b1', '0xa']:
        with open(f'./{file}/{chain_id}.json', 'r') as f:
            data = json.loads(f.read())
        for key, value in data['amount'].items():
            if key in borrows:
                borrows[key] += value
            else:
                borrows[key] = value

        for key, value in data['n_events'].items():
            if key in n_borrows:
                n_borrows[key] += value
            else:
                n_borrows[key] = value

        for key, value in data['amount_of_events'].items():
            if key in amount_of_borrows:
                amount_of_borrows[key] += value
            else:
                amount_of_borrows[key] = value

        for key, value in data['number_of_events'].items():
            if key in number_of_borrows:
                number_of_borrows[key] += value
            else:
                number_of_borrows[key] = value
    number_of_wallets = len(amount_of_borrows)
    total_number_of_borrow = sum(number_of_borrows.values())
    total_borrow = sum(amount_of_borrows.values())
    mode_amount, mode_key = BorrowAnalytic.find_mode(borrows)
    _, mode_n_key = BorrowAnalytic.find_mode(n_borrows)
    mode_n_total_borrow = 0
    for key, value in number_of_borrows.items():
        if value in n_borrows[mode_n_key]:
            mode_n_total_borrow += amount_of_borrows[key]
    with open(f"{file}/all.json", 'w') as f:
        data = {
            "events": borrows,
            "n_events": n_borrows,
            "number_of_events": number_of_borrows,
            "amount_of_events": amount_of_borrows
        }
        json.dump(data, f, indent=1)
    print("Number of wallets: ", number_of_wallets)
    print("Average amount of wallets: ", total_borrow / number_of_wallets)
    print("Mode amount: ", mode_key)
    print("Percentage mode amount: ", mode_amount / total_borrow)
    print("Average number of wallets: ", total_number_of_borrow / number_of_wallets)
    print("Mode number of borrows: ", mode_n_key)
    print("Percentage mode n borrows: ", mode_n_total_borrow / total_borrow)
    print("number of borrows:", total_number_of_borrow)
    print('Borrow amount: ', total_borrow)


class BorrowAnalytic:
    def __init__(self, mongo: MongoDB, mongo_klg: KLG, chain_id):
        self.mongo = mongo
        self.mongo_klg = mongo_klg
        self.chain_id = chain_id
        self.get_ctokens()

    @staticmethod
    def round_one_digit(n):
        n = int(n)
        return round(n, 1 - len(str(n)))

    @staticmethod
    def find_mode(data):
        mode_amount, mode_key, max_ = 0, 0, 0
        for key, value in data.items():
            if len(value) > max_:
                mode_amount = sum(value)
                mode_key = key
                max_ = len(value)
        return mode_amount, mode_key

    def get_ctokens(self):
        self.ctokens = {}
        for i in ["0x38_0xfd36e2c2a6789db23113685031d7f16329158384",
                  "0x38_0x589de0f0ccf905477646599bb3e5c622c84cc0ba",
                  "0x1_0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b"]:
            if self.chain_id in i:
                address = i.split('_')[1]
                contract = self.mongo_klg.get_smart_contract(self.chain_id, address)
                for key, value in contract['lendingInfo']['reservesList'].items():
                    self.ctokens[value['vToken']] = key

    def count_number_of_liquidate_event(self, start_timestamp, end_timestamp, wallets):
        events = self.mongo.get_documents("lending_events",
                                          {"event_type": "LIQUIDATE",
                                           "block_timestamp": {"$gte": start_timestamp, "$lt": end_timestamp}})
        liquidates = {}
        number_of_liquidates = {}
        amount_of_liquidates = {}
        prices = {}
        for event in events:
            wallet = event['user']
            if wallets and wallet not in wallets:
                continue
            if 'debt_asset' in event:
                token = event['debt_asset']
            elif event['contract_address'] in self.ctokens:
                token = self.ctokens[event['contract_address']]
            else:
                continue
            if wallet not in number_of_liquidates:
                number_of_liquidates[wallet] = 0
                amount_of_liquidates[wallet] = 0.0
            if "debt_to_cover_in_usd" in event:
                amount_in_usd = event['debt_to_cover_in_usd']
            else:
                if token not in prices:
                    token_data = self.mongo_klg.get_smart_contract(self.chain_id, token)
                    price = token_data["price"]
                    prices[token] = price
                else:
                    price = prices[token]
                amount_in_usd = event['debt_to_cover'] * price
            round_amount_in_usd = self.round_one_digit(amount_in_usd)
            if round_amount_in_usd not in liquidates:
                liquidates[round_amount_in_usd] = []
            liquidates[round_amount_in_usd].append(amount_in_usd)
            number_of_liquidates[wallet] += 1
            amount_of_liquidates[wallet] += amount_in_usd
        n_liquidates = {}
        for key, value in number_of_liquidates.items():
            round_number_of_borrows = self.round_one_digit(value)
            if round_number_of_borrows not in n_liquidates:
                n_liquidates[round_number_of_borrows] = []
            n_liquidates[round_number_of_borrows].append(value)

        number_of_wallets = len(amount_of_liquidates)
        total_number_of_liquidate = sum(number_of_liquidates.values())
        total_liquidate = sum(amount_of_liquidates.values())
        mode_amount, mode_key = self.find_mode(liquidates)
        _, mode_n_key = self.find_mode(n_liquidates)
        mode_n_total_liquidate = 0
        for key, value in number_of_liquidates.items():
            if value in n_liquidates[mode_n_key]:
                mode_n_total_liquidate += amount_of_liquidates[key]

        with open(f"./users/debtor_{self.chain_id}.json", 'w') as f:
            json.dump(list(amount_of_liquidates.keys()), f, indent=1)
        with open(f"./liquidate/{self.chain_id}.json", 'w') as f:
            data = {
                "amount": liquidates,
                "n_events": n_liquidates,
                "number_of_events": number_of_liquidates,
                "amount_of_events": amount_of_liquidates
            }
            json.dump(data, f, indent=1)
        print("Number of wallets: ", number_of_wallets)
        print("Average amount of wallets: ", total_liquidate / number_of_wallets)
        print("Average amount of events: ", total_liquidate / total_number_of_liquidate)
        print("Mode amount: ", mode_key)
        print("Percentage mode amount: ", mode_amount / total_liquidate)
        print("Average number of wallets: ", total_number_of_liquidate / number_of_wallets)
        print("Mode number of liquidates: ", mode_n_key)
        print("Percentage mode n liquidates: ", mode_n_total_liquidate / total_liquidate)
        print("Number of liquidates:", total_number_of_liquidate)
        print('Liquidate amount: ', total_liquidate)

    def count_number_of_borrow_event(self, start_timestamp, end_timestamp, wallets):
        events = self.mongo.get_documents("lending_events",
                                          {"event_type": "BORROW",
                                           "block_timestamp": {"$gte": start_timestamp, "$lt": end_timestamp}})
        borrows = {}
        number_of_borrows = {}
        amount_of_borrows = {}
        prices = {}
        for event in events:
            wallet = event['wallet']
            if wallets and wallet not in wallets:
                continue
            if 'reserve' in event:
                token = event['reserve']
            elif event['contract_address'] in self.ctokens:
                token = self.ctokens[event['contract_address']]
            else:
                continue
            if wallet not in number_of_borrows:
                number_of_borrows[wallet] = 0
                amount_of_borrows[wallet] = 0.0
            if "amount_in_usd" in event:
                amount_in_usd = event['amount_in_usd']
            else:
                if token not in prices:
                    token_data = self.mongo_klg.get_smart_contract(self.chain_id, token)
                    price = token_data["price"]
                    prices[token] = price
                else:
                    price = prices[token]
                amount_in_usd = event['amount'] * price
            round_amount_in_usd = self.round_one_digit(amount_in_usd)
            if round_amount_in_usd not in borrows:
                borrows[round_amount_in_usd] = []
            borrows[round_amount_in_usd].append(amount_in_usd)
            number_of_borrows[wallet] += 1
            amount_of_borrows[wallet] += amount_in_usd
        n_borrows = {}
        for key, value in number_of_borrows.items():
            round_number_of_borrows = self.round_one_digit(value)
            if round_number_of_borrows not in n_borrows:
                n_borrows[round_number_of_borrows] = []
            n_borrows[round_number_of_borrows].append(value)

        number_of_wallets = len(amount_of_borrows)
        with open(f"./users/borrower_{self.chain_id}.json", 'w') as f:
            json.dump(list(amount_of_borrows.keys()), f, indent=1)
        total_number_of_borrow = sum(number_of_borrows.values())
        total_borrow = sum(amount_of_borrows.values())
        mode_amount, mode_key = self.find_mode(borrows)
        _, mode_n_key = self.find_mode(n_borrows)
        mode_n_total_borrow = 0
        for key, value in number_of_borrows.items():
            if value in n_borrows[mode_n_key]:
                mode_n_total_borrow += amount_of_borrows[key]
        with open(f"./borrows/{self.chain_id}.json", 'w') as f:
            data = {
                "amount": borrows,
                "n_events": n_borrows,
                "number_of_events": number_of_borrows,
                "amount_of_events": amount_of_borrows
            }
            json.dump(data, f, indent=1)
        print("Number of wallets: ", number_of_wallets)
        print("Average amount of wallets: ", total_borrow / number_of_wallets)
        print("Average amount of events: ", total_borrow / total_number_of_borrow)
        print("Mode amount: ", mode_key)
        print("Percentage mode amount: ", mode_amount / total_borrow)
        print("Average number of wallets: ", total_number_of_borrow / number_of_wallets)
        print("Mode number of borrows: ", mode_n_key)
        print("Percentage mode n borrows: ", mode_n_total_borrow / total_borrow)
        print("number of borrows:", total_number_of_borrow)
        print('Borrow amount: ', total_borrow)

    def count_number_of_borrow_user(self, start_timestamp, end_timestamp):
        events = self.mongo.get_documents("lending_events",
                                          {"event_type": "BORROW",
                                           "block_timestamp": {"$gte": start_timestamp, "$lt": end_timestamp}})
        wallets = []
        for event in events:
            wallet = event['wallet']
            wallets.append(wallet)
        with open(f"./users/borrow_user_{self.chain_id}.json", 'w') as f:
            json.dump(wallets, f, indent=1)

    def count_number_of_liquidate_user(self, start_timestamp, end_timestamp):
        events = self.mongo.get_documents("lending_events",
                                          {"event_type": "LIQUIDATE",
                                           "block_timestamp": {"$gte": start_timestamp, "$lt": end_timestamp}})
        wallets = []
        for event in events:
            wallet = event['wallet']
            wallets.append(wallet)
        with open(f"./users/liquidate_user_{self.chain_id}.json", 'w') as f:
            json.dump(wallets, f, indent=1)


if __name__ == "__main__":
    start_time = 1690848000
    end_time = 1693526400
    with open(f"Score/Very Good.json", "r") as f:
        wallets = json.loads(f.read())
    # wallets = None
    mongo = MongoDB(connection_url="", db_prefix="ftm")
    klg = KLG(connection_url="")
    # job.count_number_of_liquidate_user(start_timestamp=start_time, end_timestamp=end_time)
    # job.count_number_of_borrow_user(start_timestamp=start_time, end_timestamp=end_time)
    # job.count_number_of_borrow_event(start_timestamp=start_time, end_timestamp=end_time, wallets=wallets)
    calculate_all_chain("borrows")
    print(".---------------------------------.")
    # job.count_number_of_liquidate_event(start_timestamp=start_time, end_timestamp=end_time, wallets=wallets)
    # calculate_all_chain("liquidate")