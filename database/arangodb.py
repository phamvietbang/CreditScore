from utils.logger_utils import get_logger
from arango import DefaultHTTPClient, ArangoClient

from config import ArangoDBConfig

logger = get_logger(__name__)


class ArangoDB:
    def __init__(
            self, connection_url=ArangoDBConfig.CONNECTION_URL
    ):
        http_client = DefaultHTTPClient()
        http_client.REQUEST_TIMEOUT = 300
        username, password, connection_url = get_connection_elements(connection_url)
        try:
            self.connection_url = connection_url
            self.client = ArangoClient(hosts=connection_url, http_client=http_client)
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")

        self.db = self.client.db(ArangoDBConfig.DATABASE, username=username, password=password)

    def get_token_price(self, key):
        query = f"""
        for sc in smart_contracts
        filter sc._key == '{key}'
        limit 1
        return {{
            "price": sc.price,
            "priceChangeLogs": sc.priceChangeLogs
        }}
        """
        data = list(self.db.aql.execute(query, count=True))
        if data:
            return data[0]
        return None

    def get_ctoken_information(self, key):
        query = f"""
        for sc in smart_contracts
        filter sc._key=="{key}"
        limit 1
        return sc
        """
        data = list(self.db.aql.execute(query, count=True))
        if data:
            return data[0]["lendingInfo"]
        return None


def get_connection_elements(string):
    """
    example output for exporter_type: exporter_type@username:password@connection_url

    :param string:
    :return: username, password, connection_url
    """
    try:
        elements = string.split("@")
        auth = elements[1].split(":")
        username = auth[0]
        password = auth[1]
        connection_url = elements[2]
        return username, password, connection_url
    except Exception as e:
        logger.warning(f"get_connection_elements err {e}")
        return None, None, None
