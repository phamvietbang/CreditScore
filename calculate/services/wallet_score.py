import math
import time

from calculate.services.statistic_service import about, sum_frequency, get_average, \
    get_return_on_investment, get_value_with_timestamp, get_standardized_score_info, get_logs_in_time
from config import get_logger
from constants import WalletStatisticFieldConstant, WalletCreditScoreWeightConstant, TimeConstant, \
    KDaysWeightConstant
from utils.utils import remove_null

logger = get_logger(__name__)


def calculate_credit_score(wallet, statistics, tokens, k, h, current_time=time.time(), return_elements=False):
    timestamp_chosen = current_time - 86400 * k
    x1, x3, x4, x1_s, x3_s, x4_s = calculate_x134(wallet, statistics, current_time, timestamp_chosen=timestamp_chosen,
                                                  h=h)
    x2, x2_s = calculate_x2(wallet, statistics, current_time, timestamp_chosen=timestamp_chosen)
    x5, x5_s = calculate_x5(wallet, tokens, current_time)
    credit_score = WalletCreditScoreWeightConstant.a1 * x1 + WalletCreditScoreWeightConstant.a2 * x2 + WalletCreditScoreWeightConstant.a3 * x3 + WalletCreditScoreWeightConstant.a4 * x4 + WalletCreditScoreWeightConstant.a5 * x5
    if return_elements:
        return int(credit_score), [x1_s, x2_s, x3_s, x4_s, x5_s]
    return int(credit_score)


def calculate_credit_score_with_info_return(wallet, statistics, tokens, k, h, current_time=time.time()):
    timestamp_chosen = current_time - 86400 * k
    x1, x3, x4, x1_s, x3_s, x4_s, info = calculate_x134(wallet, statistics, current_time,
                                                        timestamp_chosen=timestamp_chosen, h=h, return_info=True)
    x2, x2_s, x2_info = calculate_x2(wallet, statistics, current_time, timestamp_chosen=timestamp_chosen,
                                     return_info=True)
    x5, x5_s, x5_info = calculate_x5(wallet, tokens, current_time, return_info=True)

    info.update({
        'x2': x2_info,
        'x5': x5_info,
    })
    info = dict(sorted(info.items(), key=lambda x: x[0]))
    credit_score = WalletCreditScoreWeightConstant.a1 * x1 + WalletCreditScoreWeightConstant.a2 * x2 + WalletCreditScoreWeightConstant.a3 * x3 + WalletCreditScoreWeightConstant.a4 * x4 + WalletCreditScoreWeightConstant.a5 * x5
    return int(credit_score), [x1_s, x2_s, x3_s, x4_s, x5_s], info


def calculate_x134(wallet, statistics, current_time, timestamp_chosen=0, h=10, return_info=False, history=False):
    # Get wallet asset info
    balance_logs = remove_null({int(t): v for t, v in wallet.get('balanceChangeLogs', {}).items()})
    deposit_logs = remove_null({int(t): v for t, v in wallet.get('depositChangeLogs', {}).items()})
    borrow_logs = remove_null({int(t): v for t, v in wallet.get('borrowChangeLogs', {}).items()})

    if history:
        balanceInUSD = get_value_with_timestamp(balance_logs, current_time)
        depositInUSD = get_value_with_timestamp(deposit_logs, current_time)
        borrowInUSD = get_value_with_timestamp(borrow_logs, current_time)
    else:
        balanceInUSD = wallet.get('balanceInUSD', 0)
        depositInUSD = wallet.get('depositInUSD', 0)
        borrowInUSD = wallet.get('borrowInUSD', 0)

    x1_info = {
        'balance_in_usd': balanceInUSD,
        'deposit_in_usd': depositInUSD,
        'borrow_in_usd': borrowInUSD,
    }

    asset_statistics = statistics[WalletStatisticFieldConstant.total_asset]
    deposit_statistics = statistics[WalletStatisticFieldConstant.deposit]
    borrow_statistics = statistics[WalletStatisticFieldConstant.borrow]

    # x11 - Total asset
    x11 = balanceInUSD + depositInUSD - borrowInUSD
    x1_info['total_current_asset'] = x11
    if x11 < 1000:
        x11 = 0
    else:
        x11 = about(0.1 * x11)

    # x12 - Total asset average
    asset_logs = combined_asset_logs(balance_logs, deposit_logs, borrow_logs)
    average_asset, time_asset_over_threshold = get_average(list(asset_logs.values()), list(asset_logs.keys()),
                                                           current_time, timestamp_chosen,
                                                           threshold=asset_statistics['mean'] / 2)
    if average_asset < 0:
        average_asset = 0
    x1_info['average_asset'] = average_asset
    x1_info['time_asset_over_threshold'] = time_asset_over_threshold

    p = int(time_asset_over_threshold / 3600) + 1
    asset_threshold = 3621.8322162447 * math.pow(p, 1.7686217868)
    average_asset = min(average_asset, asset_threshold)
    x12 = about(asset_statistics['coefficient_a'] * pow(average_asset, asset_statistics['coefficient_b']))

    # x1 - Asset
    x1 = WalletCreditScoreWeightConstant.b11 * x11 + WalletCreditScoreWeightConstant.b12 * x12

    average_balance = get_average(list(balance_logs.values()), list(balance_logs.keys()), current_time,
                                  timestamp_chosen)
    average_borrow, time_borrow_over_threshold = get_average(list(borrow_logs.values()), list(borrow_logs.keys()),
                                                             current_time, timestamp_chosen,
                                                             threshold=borrow_statistics['mean'] / 2)
    average_deposit, time_deposit_over_threshold = get_average(list(deposit_logs.values()), list(deposit_logs.keys()),
                                                               current_time, timestamp_chosen,
                                                               threshold=deposit_statistics['mean'] / 2)

    x3_info = {}

    # x31 - Loan to value
    if average_balance <= 0:
        x31 = 0
        loan_to_balance = 0
    else:
        loan_to_balance = average_borrow / average_balance
        x31 = 1000 * max(0, 1 - max(0, -1 + loan_to_balance))
        x31 = about(x31 * time_borrow_over_threshold / (current_time - timestamp_chosen))

    x3_info['average_balance'] = average_balance
    x3_info['average_deposit'] = average_deposit
    x3_info['average_borrow'] = average_borrow
    x3_info['loan_to_balance'] = loan_to_balance
    x3_info['time_borrow_over_threshold'] = time_borrow_over_threshold

    # x32 - Loan to investment
    if average_deposit <= 0:
        x32 = 0
        x3_info['loan_to_investment'] = 0
    else:
        loan_to_investment = average_borrow / average_deposit
        x32 = about(1000 * max(0, 1 - max(0, -1 + loan_to_investment)))
        x3_info['loan_to_investment'] = loan_to_investment

    # x3 - Loan ratios
    x3 = about(x31 - (1000 - x32))

    # x4 - Circulating asset
    x4_info = {}

    # x41 - investment to total asset ratio
    investment_to_asset = average_deposit / average_asset if average_asset > 0 else 0
    x4_info['investment_per_asset'] = investment_to_asset
    x4_info['time_deposit_over_threshold'] = time_deposit_over_threshold
    x41 = about(1000 * investment_to_asset * time_deposit_over_threshold / (current_time - timestamp_chosen))

    # # x42 - Return on investment ROI
    # return_on_investment = get_return_on_investment(balance_logs, deposit_logs, current_time=current_time, h=h)
    # x4_info['ROI'] = return_on_investment
    # x42 = about((return_on_investment * 365 * 1000) / (h * 0.15))

    x4 = WalletCreditScoreWeightConstant.b41 * x41

    info = {
        'x1': x1_info,
        'x3': x3_info,
        'x4': x4_info
    }

    if return_info:
        return x1, x3, x4, [x11, x12], [x31, x32], [x41], info
    return x1, x3, x4, [x11, x12], [x31, x32], [x41]


def combined_asset_logs(balances, deposits, borrows):
    timestamps = set(list(balances.keys()) + list(deposits.keys()) + list(borrows.keys()))
    timestamps = sorted(timestamps)
    assets = {}
    balance = deposit = borrow = 0
    for t in timestamps:
        balance = balances.get(t) if t in balances else balance
        deposit = deposits.get(t) if t in deposits else deposit
        borrow = borrows.get(t) if t in borrows else borrow

        assets[t] = max(balance + deposit - borrow, 0)

    return assets


def calculate_x2(wallet, statistics, current_time=time.time(), timestamp_chosen=0, return_info=False):
    created_at = wallet.get('createdAt') or 0
    daily_transaction_amounts = remove_null({int(t): v for t, v in wallet.get('dailyTransactionAmounts', {}).items()})
    daily_frequency_of_transactions = remove_null(
        {int(t): v for t, v in wallet.get('dailyNumberOfTransactions', {}).items()})
    liquidation_logs = wallet.get('liquidationLogs', {}).get('liquidatedWallet')
    if not liquidation_logs:
        number_of_liquidation = wallet.get('numberOfLiquidation') or 0
        total_amount_of_liquidation = wallet.get('totalValueOfLiquidation') or 0
    else:
        number_of_liquidation, total_amount_of_liquidation = count_number_of_liquidation(liquidation_logs, current_time)

    x2_info = {}

    # x21 - Age of account
    age = current_time - created_at
    age_statistic = statistics[WalletStatisticFieldConstant.age_of_account]
    if age < 0:
        age = 0
    # x21 = get_tscore_with_adjust(age, age_statistic['mean'], age_statistic['std'])
    x21 = about(age_statistic['coefficient_a'] * pow(age, age_statistic['coefficient_b']))
    x2_info['age_of_account'] = age

    # x22 - transaction amount
    daily_transaction_amount = sum(
        [v for t, v in daily_transaction_amounts.items() if timestamp_chosen < t < current_time])
    if daily_transaction_amount < 0:
        daily_transaction_amount = 0
    amount_statistic = statistics[WalletStatisticFieldConstant.transaction_amount]
    # x22 = get_tscore_with_adjust(daily_transaction_amount, amount_statistic['mean'], amount_statistic['std'])
    x22 = about(amount_statistic['coefficient_a'] * pow(daily_transaction_amount, amount_statistic['coefficient_b']))
    x2_info['daily_transaction_amount'] = daily_transaction_amount

    # x23 - frequency of transaction
    daily_frequency_of_transaction = sum_frequency(
        [v for t, v in daily_frequency_of_transactions.items() if timestamp_chosen < t < current_time])
    if daily_frequency_of_transaction < 0:
        daily_frequency_of_transaction = 0
    frequency_statistic = statistics[WalletStatisticFieldConstant.frequency_of_transaction]
    # x23 = get_tscore_with_adjust(daily_frequency_of_transaction, frequency_statistic['mean'], frequency_statistic['std'])
    x23 = about(frequency_statistic['coefficient_a'] * pow(daily_frequency_of_transaction,
                                                           frequency_statistic['coefficient_b']))
    x2_info['daily_frequency_of_transaction'] = daily_frequency_of_transaction

    # x24 - number of liquidations
    x2_info['number_of_liquidation'] = number_of_liquidation
    x24 = about(1000 - 100 * number_of_liquidation)

    # x25 - total value of liquidations
    x2_info['total_amount_of_liquidation'] = total_amount_of_liquidation
    x25 = about(1000 - 0.1 * total_amount_of_liquidation)

    # x2 - Activity history
    x2 = WalletCreditScoreWeightConstant.b21 * x21 + WalletCreditScoreWeightConstant.b22 * x22 + WalletCreditScoreWeightConstant.b23 * x23 + WalletCreditScoreWeightConstant.b24 * x24 + WalletCreditScoreWeightConstant.b25 * x25
    if return_info:
        return x2, [x21, x22, x23, x24, x25], x2_info
    return x2, [x21, x22, x23, x24, x25]


def calculate_x5(wallet, tokens, current_time, return_info=False, history=False):
    if history:
        token_change_logs = wallet.get('tokenChangeLogs', {})
        wallet_tokens = []
        for token_address, logs in token_change_logs.items():
            logs = remove_null({int(t): v for t, v in logs.items()})
            v = get_value_with_timestamp(logs, current_time, default={})
            if v.get('valueInUSD', 0) > 1:
                wallet_tokens.append(token_address)
    else:
        wallet_tokens = wallet.get('tokens', {})
        wallet_tokens = [token_address for token_address, v in wallet_tokens.items() if v > 0]

    chain_id = wallet.get('chainId')
    if chain_id:
        wallet_tokens = [f'{chain_id}_{token_address}' for token_address in wallet_tokens]

    x5_info = {}
    max_token_credit = 0
    for token in wallet_tokens:
        if token in tokens.keys():
            token_credit = tokens[token]
            if token_credit > max_token_credit:
                max_token_credit = token_credit
            x5_info[token] = token_credit

    x51 = about(max_token_credit)
    x52 = 0
    x5 = WalletCreditScoreWeightConstant.b51 * x51 + WalletCreditScoreWeightConstant.b52 * x52
    if return_info:
        return x5, [x51, x52], x5_info
    return x5, [x51, x52]


def number_of_days(tokens, current_time=None):
    if current_time is None:
        current_time = int(time.time())

    params = {'price_24h': [], 'price_7d': [], 'trading_24h': [], 'trading_7d': []}

    info = {}
    total_market_cap = 0
    for t in tokens:
        market_cap_logs = {int(timestamp): v for timestamp, v in t['marketCapChangeLogs'].items()}
        market_cap = get_value_with_timestamp(market_cap_logs, timestamp=current_time, default=None)
        if market_cap is None:
            continue

        total_market_cap += market_cap

        try:
            price_logs = {int(timestamp): v for timestamp, v in t['priceChangeLogs'].items()}
            price_logs = get_logs_in_time(price_logs, end_time=current_time)

            trading_logs = {int(timestamp): v for timestamp, v in t['dailyTradingVolumes'].items()}
            trading_logs = get_logs_in_time(trading_logs, end_time=current_time)

            # price_last_updated_at = list(price_logs.keys())[-1]
            price_24h_volatility = get_stability(price_logs, start_time=current_time - TimeConstant.A_DAY)
            price_7d_volatility = get_stability(price_logs, start_time=current_time - TimeConstant.DAYS_7)

            # trading_last_updated_at = list(trading_logs.keys())[-1]
            trading_24h_volatility = get_stability(trading_logs, start_time=current_time - TimeConstant.A_DAY)
            trading_7d_volatility = get_stability(trading_logs, start_time=current_time - TimeConstant.DAYS_7)
        except Exception as ex:
            logger.exception(ex)
            total_market_cap -= market_cap
            continue

        price_24h_volatility = price_24h_volatility * market_cap
        price_7d_volatility = price_7d_volatility * market_cap
        trading_24h_volatility = trading_24h_volatility * market_cap
        trading_7d_volatility = trading_7d_volatility * market_cap

        params['price_24h'].append(price_24h_volatility)
        params['price_7d'].append(price_7d_volatility)
        params['trading_24h'].append(trading_24h_volatility)
        params['trading_7d'].append(trading_7d_volatility)

        info[t['tokenId']] = {
            'price_24h': price_24h_volatility,
            'price_7d': price_7d_volatility,
            'trading_24h': trading_24h_volatility,
            'trading_7d': trading_7d_volatility,
        }

    # with open('data/info.json', 'w') as f:
    #     json.dump(info, f)

    elements = {}
    top_3_tokens = {}
    for key, value in params.items():
        elements[key] = sum(value) / total_market_cap
        top_3_tokens[key] = dict(
            sorted({t: v[key] for t, v in info.items()}.items(), key=lambda x: x[1], reverse=True)[:3])
    u = KDaysWeightConstant.a1 * elements['price_24h'] + KDaysWeightConstant.a2 * elements[
        'price_7d'] + KDaysWeightConstant.a3 * elements['trading_24h'] + KDaysWeightConstant.a4 * elements['trading_7d']

    if u < 3:
        k = 30
    elif u > 10:
        k = 100
    else:
        k = int(10 * u)
    return k, u, elements, top_3_tokens, total_market_cap


def get_stability(change_logs, start_time):
    sub_change_logs = get_logs_in_time(change_logs, start_time=start_time)
    mean, std = get_standardized_score_info(list(sub_change_logs.values()))
    volatility = about(100 * std / mean, _min=0, _max=100) if mean != 0 else 0
    return volatility


def count_number_of_liquidation(liquidation_logs, current_time):
    _number, _amount = 0, 0
    for buyer, logs in liquidation_logs.items():
        for timestamp, value in logs.items():
            if int(timestamp) <= int(current_time):
                _number += 1
                _amount += value["debtAssetInUSD"]

    return _number, _amount
