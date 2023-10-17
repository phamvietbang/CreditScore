import time

from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

from constants.constants import TimeConstants
from constants.network_constants import Chains, Networks
from services.eth.eth_services import EthService
from utils.logger_utils import get_logger

logger = get_logger('Blocks number')


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Blocks(metaclass=SingletonMeta):
    def __init__(self):
        self.eth_services = {}
        self.blocks = {}
        for chain_id, chain_name in Chains.names.items():
            _w3 = Web3(HTTPProvider(Networks.providers[chain_name]))
            _w3.middleware_onion.inject(geth_poa_middleware, layer=0)

            self.eth_services[chain_id] = EthService(_w3)
            self.blocks[chain_id] = {}

    def block_numbers(self, chain_id, timestamps):
        if not timestamps:
            return []

        return_list = True
        if not isinstance(timestamps, list):
            timestamps = [timestamps]
            return_list = False

        if chain_id not in self.eth_services:
            raise ValueError(f'Chain {chain_id} not support')

        service = self.eth_services[chain_id]

        blocks = {}
        for timestamp in timestamps:
            if timestamp not in self.blocks:
                block_number = service.get_block_for_timestamp(timestamp)
                self.blocks[chain_id][timestamp] = BlockNumber(block_number, timestamp)

            block: BlockNumber = self.blocks[chain_id][timestamp]
            blocks[timestamp] = block.number

        self.clean(chain_id)
        return blocks if return_list else blocks[timestamps[0]]

    def clean(self, chain_id):
        expired = []
        for timestamp, block in self.blocks[chain_id].items():
            if block.is_expired:
                expired.append(timestamp)

        for timestamp in expired:
            del self.blocks[chain_id][timestamp]


class BlockNumber:
    def __init__(self, number, timestamp, ex=TimeConstants.A_DAY):
        self.number = number
        self.timestamp = timestamp
        self.expire = int(time.time()) + ex

    def is_expired(self):
        return int(time.time()) > self.expire
