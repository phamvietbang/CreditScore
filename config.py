import os
from dotenv import load_dotenv

load_dotenv()


class MongoConfig:
    HOST = os.getenv("MONGO_HOST")
    PORT = os.getenv("MONGO_PORT")
    USERNAME = os.getenv("MONGO_USERNAME")
    PASSWORD = os.getenv("MONGO_PASSWORD")
    CONNECTION_URL = os.getenv("MONGO_CONNECTION_URL") or f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}"
    DATABASE = 'LendingPools'


class ArangoDBConfig:
    ARANGODB_HOST = os.getenv("ARANGODB_HOST", '0.0.0.0')
    ARANGODB_PORT = os.getenv("ARANGODB_PORT", '8529')
    USERNAME = os.getenv("ARANGODB_USERNAME", "root")
    PASSWORD = os.getenv("ARANGODB_PASSWORD", "123")
    DATABASE = os.getenv("ARANGODB_DATABASE", "klg_database")
    CONNECTION_URL = os.getenv("ARANGODB_CONNECTION_URL") or f"http://{ARANGODB_HOST}:{ARANGODB_PORT}"
