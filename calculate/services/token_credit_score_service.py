import time

from config.config import get_logger
from config.constant import TokenCreditScoreWeightConstant
from calculate.services.statistic_service import get_tscore, get_standardized_score_info, about, \
    get_value_with_timestamp, get_logs_in_time

logger = get_logger(__file__)


def calculate_credit_score(_statistics, token, return_elements=False):
    price = token.get('price', 0)
    highest_price = token.get('priceHighest') or price
    trading_volume_24h = token.get('tradingVolume24h', 0)
    market_cap = token.get('marketCap')

    price_over_highest = price / highest_price if highest_price else 0
    trading_volume_over_market_cap = trading_volume_24h / market_cap if market_cap else None

    # x1 - Market Cap
    if market_cap:
        x1 = get_tscore(market_cap, _statistics['market_cap']['mean'], _statistics['market_cap']['std'])
    else:
        x1 = 0

    # x2 - Current price over highest price
    x2 = price_over_highest * 1000

    # x3 - Number of transactions
    # x31 - Number of transactions 24h
    n_tx_24h_statistic = _statistics['number_of_transaction_24h']
    daily_txs = token.get('dailyNumberOfTransactions') or {}
    frequency_txs = list(daily_txs.values())
    if frequency_txs:
        n_tx_24h = frequency_txs[-1]
        x31 = get_tscore(n_tx_24h, n_tx_24h_statistic['mean'], n_tx_24h_statistic['std'])
    else:
        x31 = 0

    # x32 - Number of transactions 7 days
    n_tx_7d_statistic = _statistics['number_of_transaction_7d']
    n_tx_7d = sum(frequency_txs[-7:]) if frequency_txs else 0
    if n_tx_7d:
        x32 = get_tscore(n_tx_7d, n_tx_7d_statistic['mean'], n_tx_7d_statistic['std'])
    else:
        x32 = 0

    # x33 - Number of transactions 100 days
    n_tx_statistic = _statistics['number_of_transaction_100d']
    n_transactions = sum(frequency_txs[-100:]) if frequency_txs else 0
    if n_transactions:
        x33 = get_tscore(n_transactions, n_tx_statistic['mean'], n_tx_statistic['std'])
    else:
        x33 = 0

    x3 = TokenCreditScoreWeightConstant.b31 * x31 + TokenCreditScoreWeightConstant.b32 * x32 + TokenCreditScoreWeightConstant.b33 * x33

    # x4 - Trading volume
    # x41 - Trading volume 24h over market cap
    trading_over_cap_statistic = _statistics['trading_24h_over_cap']
    if trading_volume_over_market_cap:
        x41 = get_tscore(trading_volume_over_market_cap, trading_over_cap_statistic['mean'], trading_over_cap_statistic['std'])
    else:
        x41 = 0

    # x42 - Trading volume 7 days over market cap
    trading_7d_over_cap_statistic = _statistics['trading_7d_over_cap']
    daily_trading_volumes = token.get('dailyTradingVolumes') or {}
    trading_volumes = list(daily_trading_volumes.values())
    trading_7d_over_cap = sum(trading_volumes[-672:]) / market_cap if market_cap and trading_volumes else 0
    if trading_7d_over_cap:
        x42 = get_tscore(trading_7d_over_cap, trading_7d_over_cap_statistic['mean'], trading_7d_over_cap_statistic['std'])
    else:
        x42 = 0

    # x43 - Trading volume 100 days over market cap
    trading_100d_over_cap_statistic = _statistics['trading_100d_over_cap']
    trading_100d_over_cap = sum(trading_volumes[-9600:]) / market_cap if market_cap and trading_volumes else 0
    if trading_100d_over_cap:
        x43 = get_tscore(trading_100d_over_cap, trading_100d_over_cap_statistic['mean'], trading_100d_over_cap_statistic['std'])
    else:
        x43 = 0

    # x44 - Trading volume 24h
    trading_24h_statistic = _statistics['trading_24h']
    if trading_volume_24h:
        x44 = get_tscore(trading_volume_24h, trading_24h_statistic['mean'], trading_24h_statistic['std'])
    else:
        x44 = 0

    # x45 - Trading volume 7 days
    trading_7d_statistic = _statistics['trading_7d']
    trading_volume_7d = sum(trading_volumes[-7:])
    if trading_volume_7d:
        x45 = get_tscore(trading_volume_7d, trading_7d_statistic['mean'], trading_7d_statistic['std'])
    else:
        x45 = 0

    # x46 - Trading volume 100 days
    trading_100d_statistic = _statistics['trading_100d']
    trading_volume_100d = sum(trading_volumes)
    if trading_volume_100d:
        x46 = get_tscore(trading_volume_100d, trading_100d_statistic['mean'], trading_100d_statistic['std'])
    else:
        x46 = 0

    x4 = TokenCreditScoreWeightConstant.b41 * x41 + TokenCreditScoreWeightConstant.b42 * x42 + TokenCreditScoreWeightConstant.b43 * x43 + \
        TokenCreditScoreWeightConstant.b44 * x44 + TokenCreditScoreWeightConstant.b45 * x45 + TokenCreditScoreWeightConstant.b46 * x46

    # x5 - Holders
    # x51 - Number of holders
    holders = token.get('numberOfHolder', 0)
    holders_statistic = _statistics['number_of_holder']
    if holders:
        x51 = get_tscore(holders, holders_statistic['mean'], holders_statistic['std'], log=True)
    else:
        x51 = 0

    # x52 - Market cap over number of holders
    cap_over_holders_statistic = _statistics['cap_over_holders']
    cap_over_holders = token.get('marketCap', 0) / token['holders'] if token.get('holders') else 0
    if cap_over_holders:
        x52 = get_tscore(cap_over_holders, cap_over_holders_statistic['mean'], cap_over_holders_statistic['std'])
    else:
        x52 = 0

    x5 = TokenCreditScoreWeightConstant.b51 * x51 + TokenCreditScoreWeightConstant.b52 * x52

    # x6 - Distribution of holder
    holder_distribution = token.get('holderDistribution')
    distribution_statistic = _statistics['holder_distribution']
    if holder_distribution:
        # if holder_distribution == 100:
        #     x6 = 500

        holder_distribution = - holder_distribution
        x6 = get_tscore(holder_distribution, distribution_statistic['mean'], distribution_statistic['std'])
    else:
        x6 = 0

    # x7 - Price stability
    # x71 - Price stability 24h
    x71 = about(10 * token.get('priceStability', 0))

    # x72 - Price stability 7 days
    price_change_logs = token.get('priceChangeLogs') or {}
    prices = list(price_change_logs.values())
    price_7d = prices[-672:] if prices else []
    if price_7d:
        price_stability_7d_mean, price_stability_7d_std = get_standardized_score_info(price_7d)
        if price_stability_7d_mean != 0:
            x72 = about(1000 - 1000 * price_stability_7d_std / price_stability_7d_mean)
        elif (price_stability_7d_std == 0) and (len(price_7d) > 1):
            x72 = 1000
        else:
            x72 = 0
    else:
        x72 = 0

    # x73 - Price stability 100 days
    if prices:
        price100d_mean, price100d_std = get_standardized_score_info(prices[-9600:])
        if price100d_mean != 0:
            x73 = about(1000 - 1000 * price100d_std / price100d_mean)
        elif (price100d_std == 0) and (len(prices) > 1):
            x73 = 1000
        else:
            x73 = 0
    else:
        x73 = 0

    x7 = TokenCreditScoreWeightConstant.b71 * x71 + TokenCreditScoreWeightConstant.b72 * x72 + TokenCreditScoreWeightConstant.b73 * x73

    # Token credit score
    x = TokenCreditScoreWeightConstant.a1 * x1 + TokenCreditScoreWeightConstant.a2 * x2 + TokenCreditScoreWeightConstant.a3 * x3 + TokenCreditScoreWeightConstant.a4 * x4 + \
        TokenCreditScoreWeightConstant.a5 * x5 + TokenCreditScoreWeightConstant.a6 * x6 + TokenCreditScoreWeightConstant.a7 * x7

    if return_elements:
        try:
            return int(x), [int(x1), int(x2), int(x3), int(x4), int(x5), int(x6), int(x7)]
        except Exception as ex:
            logger.info(f'{x1} - {x2} - {x3} - {x4} - {x5} - {x6} - {x7}')
            raise ex
    return int(x)


def calculate_credit_score_history(_statistics, token, current_time=int(time.time()), return_elements=False):
    price_logs = {int(t): v for t, v in token.get('priceChangeLogs', {}).items()}
    price = get_value_with_timestamp(price_logs, timestamp=current_time)
    highest_price = token.get('priceHighest') or price

    daily_tradings = {int(t): v for t, v in token.get('dailyTradingVolumes', {}).items()}
    trading_volume_24h = get_value_with_timestamp(daily_tradings, timestamp=current_time)

    market_cap_logs = {int(t): v for t, v in token.get('marketCapChangeLogs', {}).items()}
    market_cap = get_value_with_timestamp(market_cap_logs, timestamp=current_time)

    price_over_highest = price / highest_price if highest_price else 0
    trading_volume_over_market_cap = trading_volume_24h / market_cap if market_cap else None

    # x1 - Market Cap
    if market_cap:
        x1 = get_tscore(market_cap, _statistics['market_cap']['mean'], _statistics['market_cap']['std'])
    else:
        x1 = 0

    # x2 - Current price over highest price
    x2 = price_over_highest * 1000

    # x3 - Number of transactions
    # x31 - Number of transactions 24h
    n_tx_24h_statistic = _statistics['number_of_transaction_24h']
    daily_txs = {int(t): v for t, v in token.get('dailyNumberOfTransactions', {}).items()}
    daily_txs = get_logs_in_time(daily_txs, end_time=current_time)
    frequency_txs = list(daily_txs.values())
    if frequency_txs:
        n_tx_24h = frequency_txs[-1]
        x31 = get_tscore(n_tx_24h, n_tx_24h_statistic['mean'], n_tx_24h_statistic['std'])
    else:
        x31 = 0

    # x32 - Number of transactions 7 days
    n_tx_7d_statistic = _statistics['number_of_transaction_7d']
    frequency_txs_7d = get_logs_in_time(daily_txs, start_time=current_time - 7 * 86400)
    n_tx_7d = sum(list(frequency_txs_7d.values())) if frequency_txs_7d else 0
    if n_tx_7d:
        x32 = get_tscore(n_tx_7d, n_tx_7d_statistic['mean'], n_tx_7d_statistic['std'])
    else:
        x32 = 0

    # x33 - Number of transactions 100 days
    n_tx_statistic = _statistics['number_of_transaction_100d']
    frequency_txs_100d = get_logs_in_time(daily_txs, start_time=current_time - 100 * 86400)
    n_tx_100d = sum(list(frequency_txs_100d.values())) if frequency_txs_100d else 0
    if n_tx_100d:
        x33 = get_tscore(n_tx_100d, n_tx_statistic['mean'], n_tx_statistic['std'])
    else:
        x33 = 0

    x3 = TokenCreditScoreWeightConstant.b31 * x31 + TokenCreditScoreWeightConstant.b32 * x32 + TokenCreditScoreWeightConstant.b33 * x33

    # x4 - Trading volume
    # x41 - Trading volume 24h over market cap
    trading_over_cap_statistic = _statistics['trading_24h_over_cap']
    if trading_volume_over_market_cap:
        x41 = get_tscore(trading_volume_over_market_cap, trading_over_cap_statistic['mean'], trading_over_cap_statistic['std'])
    else:
        x41 = 0

    # x42 - Trading volume 7 days over market cap
    trading_7d_over_cap_statistic = _statistics['trading_7d_over_cap']
    daily_trading_volumes = {int(t): v for t, v in token.get('dailyTradingVolumes', {}).items()}
    daily_trading_volumes_7d = get_logs_in_time(daily_trading_volumes, start_time=current_time - 7 * 86400, end_time=current_time)
    trading_7d_over_cap = sum(list(daily_trading_volumes_7d.values())) / market_cap if market_cap and daily_trading_volumes_7d else 0
    if trading_7d_over_cap:
        x42 = get_tscore(trading_7d_over_cap, trading_7d_over_cap_statistic['mean'], trading_7d_over_cap_statistic['std'])
    else:
        x42 = 0

    # x43 - Trading volume 100 days over market cap
    trading_100d_over_cap_statistic = _statistics['trading_100d_over_cap']
    daily_trading_volumes_100d = get_logs_in_time(daily_trading_volumes, start_time=current_time - 100 * 86400, end_time=current_time)
    trading_100d_over_cap = sum(list(daily_trading_volumes_100d.values())) / market_cap if market_cap and daily_trading_volumes_100d else 0
    if trading_100d_over_cap:
        x43 = get_tscore(trading_100d_over_cap, trading_100d_over_cap_statistic['mean'], trading_100d_over_cap_statistic['std'])
    else:
        x43 = 0

    # x44 - Trading volume 24h
    trading_24h_statistic = _statistics['trading_24h']
    if trading_volume_24h:
        x44 = get_tscore(trading_volume_24h, trading_24h_statistic['mean'], trading_24h_statistic['std'])
    else:
        x44 = 0

    # x45 - Trading volume 7 days
    trading_7d_statistic = _statistics['trading_7d']
    trading_volume_7d = sum(list(daily_trading_volumes_7d.values()))
    if trading_volume_7d:
        x45 = get_tscore(trading_volume_7d, trading_7d_statistic['mean'], trading_7d_statistic['std'])
    else:
        x45 = 0

    # x46 - Trading volume 100 days
    trading_100d_statistic = _statistics['trading_100d']
    trading_volume_100d = sum(list(daily_trading_volumes_100d.values()))
    if trading_volume_100d:
        x46 = get_tscore(trading_volume_100d, trading_100d_statistic['mean'], trading_100d_statistic['std'])
    else:
        x46 = 0

    x4 = TokenCreditScoreWeightConstant.b41 * x41 + TokenCreditScoreWeightConstant.b42 * x42 + TokenCreditScoreWeightConstant.b43 * x43 + \
        TokenCreditScoreWeightConstant.b44 * x44 + TokenCreditScoreWeightConstant.b45 * x45 + TokenCreditScoreWeightConstant.b46 * x46

    # x5 - Holders
    # x51 - Number of holders
    holders_logs = {int(t): v for t, v in token.get('numberOfHolderChangeLogs', {}).items()}
    holders = get_value_with_timestamp(holders_logs, timestamp=current_time)
    holders_statistic = _statistics['number_of_holder']
    if holders:
        x51 = get_tscore(holders, holders_statistic['mean'], holders_statistic['std'], log=True)
    else:
        x51 = 0

    # x52 - Market cap over number of holders
    cap_over_holders_statistic = _statistics['cap_over_holders']
    cap_over_holders = market_cap / holders if holders else 0
    if cap_over_holders:
        x52 = get_tscore(cap_over_holders, cap_over_holders_statistic['mean'], cap_over_holders_statistic['std'])
    else:
        x52 = 0

    x5 = TokenCreditScoreWeightConstant.b51 * x51 + TokenCreditScoreWeightConstant.b52 * x52

    # x6 - Distribution of holder
    holder_distribution_logs = {int(t): v for t, v in token.get('holderDistributionChangeLogs', {}).items()}
    holder_distribution = get_value_with_timestamp(holder_distribution_logs, timestamp=current_time)
    distribution_statistic = _statistics['holder_distribution']
    if holder_distribution:
        # if holder_distribution == 100:
        #     x6 = 500

        holder_distribution = - holder_distribution
        x6 = get_tscore(holder_distribution, distribution_statistic['mean'], distribution_statistic['std'])
    else:
        x6 = 0

    # x7 - Price stability
    # x71 - Price stability 24h
    price_stability_logs = {int(t): v for t, v in token.get('priceStabilityChangeLogs', {}).items()}
    price_stability = get_value_with_timestamp(price_stability_logs, timestamp=current_time, default=0)
    x71 = about(10 * price_stability)

    # x72 - Price stability 7 days
    price_logs_7d = get_logs_in_time(price_logs, start_time=current_time - 7 * 86400, end_time=current_time)
    price_7d = list(price_logs_7d.values()) if price_logs_7d else []
    if price_7d:
        price_stability_7d_mean, price_stability_7d_std = get_standardized_score_info(price_7d)
        if price_stability_7d_mean != 0:
            x72 = about(1000 - 1000 * price_stability_7d_std / price_stability_7d_mean)
        elif (price_stability_7d_std == 0) and (len(price_7d) > 1):
            x72 = 1000
        else:
            x72 = 0
    else:
        x72 = 0

    # x73 - Price stability 100 days
    price_logs_100d = get_logs_in_time(price_logs, start_time=current_time - 100 * 86400, end_time=current_time)
    price_100d = list(price_logs_100d.values()) if price_logs_100d else []
    if price_100d:
        price100d_mean, price100d_std = get_standardized_score_info(price_100d)
        if price100d_mean != 0:
            x73 = about(1000 - 1000 * price100d_std / price100d_mean)
        elif (price100d_std == 0) and (len(price_100d) > 1):
            x73 = 1000
        else:
            x73 = 0
    else:
        x73 = 0

    x7 = TokenCreditScoreWeightConstant.b71 * x71 + TokenCreditScoreWeightConstant.b72 * x72 + TokenCreditScoreWeightConstant.b73 * x73

    # Token credit score
    x = TokenCreditScoreWeightConstant.a1 * x1 + TokenCreditScoreWeightConstant.a2 * x2 + TokenCreditScoreWeightConstant.a3 * x3 + TokenCreditScoreWeightConstant.a4 * x4 + \
        TokenCreditScoreWeightConstant.a5 * x5 + TokenCreditScoreWeightConstant.a6 * x6 + TokenCreditScoreWeightConstant.a7 * x7

    if return_elements:
        try:
            return int(x), [int(x1), int(x2), int(x3), int(x4), int(x5), int(x6), int(x7)]
        except Exception as ex:
            logger.info(f'{x1} - {x2} - {x3} - {x4} - {x5} - {x6} - {x7}')
            raise ex
    return int(x)
