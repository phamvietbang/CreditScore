import time

from abi.vtoken import VTOKEN_ABI
from abi.erc20_abi import ERC20_ABI
from database.klg_mongodb import MongoDbKLG
from database.mongodb import MongoDB
from database.memory_storage import MemoryStorage
from constants.constants import Amount, CompoundForks, RemoveToken
from job.cli_job import CLIJob
from utils.logger_utils import get_logger
from web3 import Web3, HTTPProvider

logger = get_logger("Liquidated Wallet")


class ExportLiquidatedWalletJob(CLIJob):
    def __init__(
            self,
            importer: MongoDB,
            exporter: MongoDB,
            token_db: MongoDB,
            arangodb: MongoDbKLG,
            provider_uri,
            batch_size,
            chain_id):
        super().__init__()
        self.pool_name = {
            "0x75de5f7c91a89c16714017c7443eca20c7a8c295": "trava",
            "0xe29a55a6aeff5c8b1beede5bcf2f0cb3af8f91f5": "valas",
            "0xd61afaaa8a69ba541bc4db9c9b40d4142b43b9a4": "trava",
            "0xd98bb590bdfabf18c164056c185fbb6be5ee643f": "trava",
            "0x9fad24f572045c7869117160a571b2e50b10d068": "geist",
            "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9": "aave",
            '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2': "aave",
            '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf': "aave",
            '0x794a61358d6845594f94dc1db02a252b5b4814ad': "aave",
            '0x4f01aed16d97e3ab5ab2b501154dc9bb0f1a5a2c': "aave",
        }
        self.ctoken_addresses = None
        self.exchange_rate = None
        self.underlying = {}
        self.arangodb = arangodb
        self.batch_size = batch_size
        self.token_db = token_db
        self.exporter = exporter
        self.importer = importer
        self.chain_id = chain_id

        self.web3 = Web3(HTTPProvider(provider_uri))
        self.local_storage = MemoryStorage.get_instance()
        self.get_underlying_ctoken()
        self.get_exchange_rate()

    def _execute(self, *args, **kwargs):
        logger.info("Start crawling...")
        cursor = self.importer.get_documents(
            collection="lending_events",
            conditions={
                "event_type": "LIQUIDATE",
                "block_timestamp": {"$gte": 1688169600, "$lt": 1696118400},
                "contract_address": {"$in": [
                    '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9',
                    '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2',
                    '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf',
                    '0x794a61358d6845594f94dc1db02a252b5b4814ad',
                    '0x4f01aed16d97e3ab5ab2b501154dc9bb0f1a5a2c',
                ]}
            }
        )
        liquidators, debtors = [], []
        count = 0
        begin = time.time()
        for event in cursor:
            self.exporter.update_document("lending_events", event)
            if "debt_asset" not in event:
                event["debt_asset"] = self.underlying[event["contract_address"]]
            if event["collateral_asset"] in self.underlying:
                event["collateral_asset"] = self.underlying[event["collateral_asset"]]

            if event["debt_asset"] in RemoveToken.tokens or event["collateral_asset"] in RemoveToken.tokens:
                continue

            amount_in_usd = {}
            for key in Amount.all:
                if key in event:
                    amount_in_usd[key] = event[key]
                elif key == Amount.liquidated_collateral_amount_in_usd:
                    amount_in_usd[key] = event[Amount.mapping[key]] * \
                                         self.exchange_rate.get(event['collateral_asset'], 1) * \
                                         self.get_token_price(event[Amount.token[key]], event["block_timestamp"])
                else:
                    amount_in_usd[key] = event[Amount.mapping[key]] * \
                                         self.get_token_price(event[Amount.token[key]], event["block_timestamp"])
            liquidator = {
                "_id": event["wallet"],
                "debtors": {
                    event["user"]: {
                        str(event["block_timestamp"]): {
                            "protocol": event["contract_address"],
                            "protocolName": self.pool_name[event["contract_address"]],
                            "collateralAsset": event["collateral_asset"],
                            "collateralAmount": event["liquidated_collateral_amount"],
                            "collateralAssetInUSD": amount_in_usd[Amount.liquidated_collateral_amount_in_usd],
                            "debtor": event["user"],
                            "blockNumber": event["block_number"]
                        }
                    }
                }
            }
            debtor = {
                "_id": event["user"],
                "buyers": {
                    event["wallet"]: {
                        str(event["block_timestamp"]): {
                            "protocol": event["contract_address"],
                            "protocolName": self.pool_name[event["contract_address"]],
                            "debtAsset": event["debt_asset"],
                            "debtAmount": event["debt_to_cover"],
                            "debtAssetInUSD": amount_in_usd[Amount.debt_to_cover_in_usd],
                            "buyer": event["wallet"],
                            "blockNumber": event["block_number"]
                        }
                    }
                }
            }
            liquidators.append(liquidator)
            debtors.append(debtor)
            if len(liquidators) == self.batch_size:
                count += self.batch_size
                logger.info(f"Export {count} events in {time.time() - begin}s")
                self.exporter.update_documents("liquidators", liquidators)
                self.exporter.update_documents("debtors", debtors)
                liquidators, debtors = [], []

        count += len(liquidators)
        logger.info(f"Export {count} events in {time.time() - begin}s")
        self.exporter.update_documents("liquidators", liquidators)
        self.exporter.update_documents("debtors", debtors)

    def get_exchange_rate(self):
        self.exchange_rate = {}
        for i in self.underlying:
            decimals = 18 + self.decimals[self.underlying[i]] - self.decimals[i]
            contract = self.web3.eth.contract(
                address=self.web3.toChecksumAddress(i), abi=VTOKEN_ABI)
            self.exchange_rate[self.underlying[i]] = contract.functions.exchangeRateCurrent().call() / 10 ** decimals

    def get_token_price(self, token, time_):
        key = f"{self.chain_id}_{token}"
        price = self.local_storage.get(key)
        if not price:
            price = self.arangodb.get_smart_contract(key)
        if not price:
            price = self.token_db.get_document(
                "token_price",
                {"_id": key}
            )

        result = price["price"]
        if "priceChangeLogs" in price and price["priceChangeLogs"]:
            for timestamp in price["priceChangeLogs"]:
                result = price["priceChangeLogs"][timestamp]
                if int(timestamp) >= time_:
                    break
        if result is None:
            print(token)
            result = 1
        self.local_storage.set(key, price)

        return result

    def get_underlying_ctoken(self):
        self.underlying = {}
        self.ctoken_addresses = {}
        self.decimals = {}
        for key in CompoundForks.chain.get(self.chain_id, {}):
            data = self.arangodb.get_ctoken_information(CompoundForks.mapping[key])
            for token in data["reservesList"]:
                ctoken = data['reservesList'][token]['vToken']
                self.underlying[ctoken] = token
                self.ctoken_addresses[token] = ctoken
                self.pool_name[ctoken] = key
                if token == '0x0000000000000000000000000000000000000000':
                    self.decimals[token] = 18
                else:
                    underlying = self.web3.eth.contract(address=self.web3.toChecksumAddress(token), abi=ERC20_ABI)
                    self.decimals[token] = underlying.functions.decimals().call()
                pool = self.web3.eth.contract(address=self.web3.toChecksumAddress(ctoken), abi=ERC20_ABI)
                self.decimals[ctoken] = pool.functions.decimals().call()
