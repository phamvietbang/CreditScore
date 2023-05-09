import time

from pycoingecko import CoinGeckoAPI

from config.config import get_logger

logger = get_logger('Market Service')


class MarketService:
    def __init__(self, platform='binance-smart-chain', currency='usd'):
        self.coingecko = CoinGeckoAPI()
        self.platform = platform
        self.currency = currency

    def get_list_coin_id(self):
        tokens = self.coingecko.get_coins_list()
        coin_ids = [t['id'] for t in tokens]
        return coin_ids

    def get_list_coin_id_by_address(self, addresses):
        results = {}
        tokens = self.coingecko.get_coins_list(include_platform=True)
        for token in tokens:
            if self.platform in token['platforms'] and token['platforms'][self.platform] != '':
                address = token['platforms'][self.platform].strip()
                if address in addresses:
                    results[address] = token["id"]
        if '0x' in addresses:
            results['0x'] = 'binancecoin'
        return results

    def get_market_info(self, ids, batch_size=200):
        length = len(ids)
        tokens = []
        for i in range(0, length, batch_size):
            last = min(i + batch_size, length)
            try:
                tokens += self.coingecko.get_coins_markets(vs_currency=self.currency, ids=ids[i:last])
            except Exception as ex:
                logger.warning(ex)
                time.sleep(10)
                try:
                    tokens += self.coingecko.get_coins_markets(vs_currency=self.currency, ids=ids[i:last])
                except Exception as ex:
                    logger.exception(ex)
                    time.sleep(10)
            time.sleep(1)
        return tokens

    def get_market_info_(self, coin_id):
        return self.coingecko.get_coins_markets(vs_currency=self.currency, ids=coin_id)

    def get_price(self, address):
        address = address.lower()
        try:
            info = self.coingecko.get_token_price(self.platform, address, self.currency)
            if not info:
                if address == '0x':
                    coin_id = 'binancecoin'
                    info = self.coingecko.get_price(coin_id, self.currency)
                    address = coin_id
                else:
                    return None
            price = info[address][self.currency]
        except Exception as ex:
            logger.exception(ex)
            return None
        return price

    def get_token_categories(self, coin_id):
        try:
            data = self.coingecko.get_coin_by_id(coin_id, localization=False, tikers=False, market_data=False, community_data=False, developer_data=False)
            categories = data.get('categories') or []
        except Exception as ex:
            logger.error(ex)
            logger.warning(f'Get categories fail with coin {coin_id}')
            categories = []

        categories.append('Token')
        return categories
