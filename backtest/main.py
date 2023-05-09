from backtest.services.liquidate_data import LiquidateDataProcessors
from config import ArangoDBConfig, MongoConfig
from database.arangodb_klg import ArangoDbKLG
from database.mongodb import MongoDB

arangodb = ArangoDbKLG(ArangoDBConfig.CONNECTION_URL)
mongodb = MongoDB(MongoConfig.CONNECTION_URL, 'blockchain_etl', 'ethereum')
processor = LiquidateDataProcessors(arangodb, mongodb, '0x1')

data = processor.calculate_liquidate_debt_amount()