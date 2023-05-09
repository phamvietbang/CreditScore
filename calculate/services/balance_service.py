import logging

from web3 import Web3

from artifacts.trava_vault import VAULT
from calculate.services.market_service import MarketService


def check_balance_of(addresses, url="https://bsc-dataseed.binance.org/"):
    w3 = Web3(Web3.HTTPProvider(url))
    market_service = MarketService()

    contract_addresses = {
        '0x14Ef31b1EFe9DDe85FcD60b0a23945B955888181': 'binance-usd',
        '0xd5Cc214621395686B972DDE8481a7463A0DaB962': 'oraichain-token',
        '0x92b3C5403f8D5F47Ff4706e477DB6eA8D397D8D7': 'wbnb'
    }
    market_info = market_service.get_market_info(list(contract_addresses.values()))
    prices = {}
    for m in market_info:
        prices[m['id']] = m.get('current_price')

    logging.info(f'Token price: {prices}')

    contracts = []
    for contract_address in contract_addresses.keys():
        contract_address = Web3.toChecksumAddress(contract_address)
        contract = w3.eth.contract(abi=VAULT, address=contract_address)
        contracts.append(contract)

    holders = []
    for address in addresses:
        address = Web3.toChecksumAddress(address)
        total_value = 0
        for contract in contracts:
            try:
                value = contract.functions.balanceOf(address).call()
                total_value += value * prices[contract_addresses.get(contract.address)] / 10 ** 18
            except Exception as e:
                logging.warning(f"Err in call smart contract {e}")
        # print(f" {address} - value - {total_value}")
        if total_value > 10:
            holders.append(str(address).lower())

    return holders


def update_wallet_asset_info(wallet, tokens_price, timestamp):
    updated = {'address': wallet['address']}

    tokens = wallet.get('tokens') or {}
    balance_in_usd = 0
    token_change_logs = {}
    for token, amount in tokens.items():
        if token in tokens_price:
            value = amount * tokens_price[token]
            token_change_logs[token] = {timestamp: {'amount': amount, 'valueInUSD': value}}
            balance_in_usd += value

    if balance_in_usd > 0:
        updated.update({
            'balanceInUSD': balance_in_usd,
            'tokenChangeLogs': token_change_logs,
            'balanceChangeLogs': {timestamp: balance_in_usd}
        })

    for type_ in ['deposit', 'borrow']:
        tokens = wallet.get(f'{type_}Tokens') or {}
        value_in_usd = 0
        change_logs = {}
        for token, amount in tokens.items():
            if token in tokens_price:
                value = amount * tokens_price[token]
                change_logs[token] = {timestamp: {'amount': amount, 'valueInUSD': value}}
                value_in_usd += value

        if value_in_usd > 0:
            updated.update({
                f'{type_}InUSD': value_in_usd,
                f'{type_}TokenChangeLogs': change_logs,
                f'{type_}ChangeLogs': {timestamp: value_in_usd}
            })
    return updated
