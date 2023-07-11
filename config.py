import logging
import os
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv

load_dotenv()


class TokenMongoDBConfig:
    MONGODB_HOST = os.environ.get("TOKEN_MONGO_HOST", '0.0.0.0')
    MONGODB_PORT = os.environ.get("TOKEN_MONGO_PORT", '8529')
    HOST = f"http://{MONGODB_HOST}:{MONGODB_PORT}"
    USERNAME = os.environ.get("TOKEN_MONGO_USERNAME", "root")
    PASSWORD = os.environ.get("TOKEN_MONGO_PASSWORD", "dev123")
    CONNECTION_URL = os.environ.get(
        "TOKEN_MONGO_CONNECTION_URL") or f'mongodb://{USERNAME}:{PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}'

class PostgresqlDBConfig:
    POSTGRESQLDB_HOST = os.environ.get("POSTGRESQLDB_HOST", '127.0.0.1')
    POSTGRESQLDB_PORT = os.environ.get("POSTGRESQLDB_PORT", '5432')
    POSTGRESQLDB_DATABASE = os.environ.get("POSTGRESQLDB_DATABASE", 'postgres')
    POSTGRESQLDB_USER = os.environ.get("POSTGRESQLDB_USER", 'postgres')
    POSTGRESQLDB_PASSWORD = os.environ.get("POSTGRESQLDB_PASSWORD", 'password')
    POSTGRESQLDB_TABLE = os.environ.get("POSTGRESQLDB_TABLE", 'events')
    CONNECTION_URL = os.environ.get("POSTGRESQLDB_CONNECTION_URL", 'url')
    SCHEMA = os.environ.get("POSTGRES_SCHEMA", "public")

class MongoConfig:
    HOST = os.getenv("MONGO_HOST")
    PORT = os.getenv("MONGO_PORT")
    USERNAME = os.getenv("MONGO_USERNAME")
    PASSWORD = os.getenv("MONGO_PASSWORD")
    CONNECTION_URL = os.getenv("MONGO_CONNECTION_URL") or f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}"
    DATABASE = 'LendingPools'

class MongoDbKLGConfig:
    HOST = "http://localhost:8529"
    USERNAME = "root"
    PASSWORD = "dev123"
    # KLG_DATABASE = "klg_database"
    KLG_DATABASE = "knowledge_graph"
    KLG = "knowledge_graph"
    WALLETS = 'wallets'
    MULTICHAIN_WALLETS = 'multichain_wallets'
    DEPOSITS = 'deposits'
    BORROWS = 'borrows'
    REPAYS = 'repays'
    WITHDRAWS = 'withdraws'
    LIQUIDATES = 'liquidates'
    SMART_CONTRACTS = 'smart_contracts'

class ArangoDBConfig:
    ARANGODB_HOST = os.getenv("ARANGODB_HOST", '0.0.0.0')
    ARANGODB_PORT = os.getenv("ARANGODB_PORT", '8529')
    USERNAME = os.getenv("ARANGODB_USERNAME", "root")
    PASSWORD = os.getenv("ARANGODB_PASSWORD", "123")
    DATABASE = os.getenv("ARANGODB_DATABASE", "klg_database")
    CONNECTION_URL = os.getenv("ARANGODB_CONNECTION_URL") or f"http://{ARANGODB_HOST}:{ARANGODB_PORT}"
    # KLG_DATABASE = "klg_database"
    KLG_DATABASE = 'knowledge_graph'
    TOKEN_DATABASE = "TokenDatabase"
    KLG = "knowledge_graph"
    TOKEN_GRAPH = "token_graph"
    WALLETS = 'wallets'
    MULTICHAIN_WALLETS = 'multichain_wallets'
    WALLET_SCORES = 'wallets_credit_scores'
    MULTICHAIN_WALLET_SCORES = 'multichain_wallets_credit_scores'
    MERGED_WALLET_SCORES = 'merged_wallets_credit_scores'
    POOLS = 'pools'
    TOKENS = 'tokens'
    MERGED_TOKENS = 'merged_tokens'
    TRANSFERS = 'transfers'
    DEPOSITS = 'deposits'
    BORROWS = 'borrows'
    REPAYS = 'repays'
    WITHDRAWS = 'withdraws'
    LIQUIDATES = 'liquidates'
    CREDIT_SCORE_CONFIGS = 'credit_score_configs'
    TOKEN_CREDIT_SCORE = 'token_credit_score'
    TOKEN_CREDIT_SCORE_X1 = 'token_credit_score_x1'
    TOKEN_CREDIT_SCORE_X2 = 'token_credit_score_x2'
    TOKEN_CREDIT_SCORE_X3 = 'token_credit_score_x3'
    TOKEN_CREDIT_SCORE_X4 = 'token_credit_score_x4'
    TOKEN_CREDIT_SCORE_X5 = 'token_credit_score_x5'
    TOKEN_CREDIT_SCORE_X6 = 'token_credit_score_x6'
    TOKEN_CREDIT_SCORE_X7 = 'token_credit_score_x7'
    MULTICHAIN_WALLETS_SIZE = 'multichain_wallets_size'

    BATCH_SIZE = 1000


DEFAULT_CREDIT_SCORE = 105
MAX_CREDIT_SCORE = 1000
MIN_CREDIT_SCORE = 0

FORMATTER = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s',
                              datefmt='%m-%d-%Y %H:%M:%S %Z')
LOG_FILE = 'logging.log'


def get_console_handler():
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2000, backupCount=1)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    # logger.addHandler(get_file_handler())
    return logger
