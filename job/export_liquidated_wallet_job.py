import time

from database.arangodb import ArangoDB
from database.mongodb import MongoDB
from database.memory_storage import MemoryStorage
from constants import Amount, CompoundForks
from job.cli_job import CLIJob
from utils.logger_utils import get_logger

logger = get_logger("Liquidated Wallet")


class ExportLiquidatedWalletJob(CLIJob):
    def __init__(
            self, importer: MongoDB, exporter: MongoDB, token_db: MongoDB, arangodb: ArangoDB, batch_size, chain_id):
        super().__init__()
        self.underlying = {}
        self.arangodb = arangodb
        self.batch_size = batch_size
        self.token_db = token_db
        self.exporter = exporter
        self.importer = importer
        self.chain_id = chain_id
        self.local_storage = MemoryStorage.get_instance()
        self.get_underlying_ctoken()

    def _execute(self, *args, **kwargs):
        logger.info("Start crawling...")
        cursor = self.importer.get_documents(
            collection="lending_events",
            conditions={
                "event_type": "LIQUIDATE"
            }
        )
        liquidators, debtors = [], []
        count = 0
        begin = time.time()
        for event in cursor:
            if "debt_asset" not in event:
                event["debt_asset"] = self.underlying[event["contract_address"]]
            if event["collateral_asset"] in self.underlying:
                event["collateral_asset"] = self.underlying[event["collateral_asset"]]
            amount_in_usd = {}
            for key in Amount.all:
                if key in event:
                    amount_in_usd[key] = event[key]
                else:
                    amount_in_usd[key] = event[Amount.mapping[key]] * self.get_token_price(
                        event[Amount.token[key]], event["block_timestamp"])
            liquidator = {
                "_id": event["wallet"],
                "debtors": {
                    event["user"]: {
                        str(event["block_timestamp"]): {
                            "protocol": event["contract_address"],
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

    def get_token_price(self, token, time_):
        key = f"{self.chain_id}_{token}"
        price = self.local_storage.get(key)
        if not price:
            price = self.token_db.get_document(
                "token_price",
                {"_id": key}
            )
        if not price:
            price = self.arangodb.get_token_price(key)
        result = price["price"]
        if "priceChangeLogs" in price and price["priceChangeLogs"]:
            for timestamp in price["priceChangeLogs"]:
                result = price["priceChangeLogs"][timestamp]
                if int(timestamp) >= time_:
                    break
        if result is None:
            result = 0
        self.local_storage.set(key, price)

        return result

    def get_underlying_ctoken(self):
        self.underlying = {}
        for key in CompoundForks.chain[self.chain_id]:
            data = self.arangodb.get_ctoken_information(CompoundForks.mapping[key])
            for token in data["reservesList"]:
                self.underlying[data['reservesList'][token]['vToken']] = token
