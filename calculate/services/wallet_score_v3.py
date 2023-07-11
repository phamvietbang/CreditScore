import math
import time

from calculate.services.statistic_service import about, sum_frequency, get_average, \
    get_return_on_investment, get_value_with_timestamp, get_standardized_score_info, get_logs_in_time
from config import get_logger
from constants.constants import WalletStatisticFieldConstant, WalletCreditScoreWeightConstantV3, TimeConstant, \
    KDaysWeightConstant
from utils.utils import remove_null

logger = get_logger(__name__)


def calculate_credit_score(wallet, statistics, tokens, k, h, current_time=time.time(), return_elements=False):
    timestamp_chosen = current_time - 86400 * k
    x1, x5, x6, x1_s, x5_s, x6_s = calculate_x156(wallet, statistics, current_time, timestamp_chosen=timestamp_chosen,
                                                  h=h)
    x2, x3, x4, x2_s, x3_s, x4_s = calculate_x234(wallet, statistics, current_time, timestamp_chosen=timestamp_chosen)
    x7, x7_s = calculate_x7(wallet, tokens, current_time)
    credit_score = WalletCreditScoreWeightConstantV3.a1 * x1 + \
                   WalletCreditScoreWeightConstantV3.a2 * x2 + \
                   WalletCreditScoreWeightConstantV3.a3 * x3 + \
                   WalletCreditScoreWeightConstantV3.a4 * x4 + \
                   WalletCreditScoreWeightConstantV3.a5 * x5 + \
                   WalletCreditScoreWeightConstantV3.a6 * x6 + \
                   WalletCreditScoreWeightConstantV3.a7 * x7
    if return_elements:
        return int(credit_score), [x1_s, x2_s, x3_s, x4_s, x5_s, x6_s, x7_s]
    return int(credit_score)


def calculate_credit_score_with_info_return(wallet, statistics, tokens, k, h, current_time=time.time()):
    timestamp_chosen = current_time - 86400 * k
    x1, x5, x6, x1_s, x5_s, x6_s, info = calculate_x156(wallet, statistics, current_time,
                                                        timestamp_chosen=timestamp_chosen, h=h, return_info=True)
    x2, x3, x4, x2_s, x3_s, x4_s, x2_info, x3_info, x4_info = calculate_x234(
        wallet, statistics, current_time, timestamp_chosen=timestamp_chosen, return_info=True)
    x7, x7_s, x7_info = calculate_x7(wallet, tokens, current_time, return_info=True)

    info.update({
        'x2': x2_info,
        'x3': x3_info,
        'x4': x4_info,
        'x7': x7_info,
    })
    info = dict(sorted(info.items(), key=lambda x: x[0]))
    credit_score = WalletCreditScoreWeightConstantV3.a1 * x1 + \
                   WalletCreditScoreWeightConstantV3.a2 * x2 + \
                   WalletCreditScoreWeightConstantV3.a3 * x3 + \
                   WalletCreditScoreWeightConstantV3.a4 * x4 + \
                   WalletCreditScoreWeightConstantV3.a5 * x5 + \
                   WalletCreditScoreWeightConstantV3.a6 * x6 + \
                   WalletCreditScoreWeightConstantV3.a7 * x7
    return int(credit_score), [x1_s, x2_s, x3_s, x4_s, x5_s, x6_s, x7_s], info


def calculate_x156(wallet, statistics, current_time, timestamp_chosen=0, h=10, return_info=False, history=False):
    # Get wallet asset info
    balance_logs = remove_null({int(t): v for t, v in wallet.get('balanceChangeLogs', {}).items()})
    deposit_logs = remove_null({int(t): v for t, v in wallet.get('depositChangeLogs', {}).items()})
    borrow_logs = remove_null({int(t): v for t, v in wallet.get('borrowChangeLogs', {}).items()})

    if history:
        balance_in_usd = get_value_with_timestamp(balance_logs, current_time)
        deposit_in_usd = get_value_with_timestamp(deposit_logs, current_time)
        borrow_in_usd = get_value_with_timestamp(borrow_logs, current_time)
    else:
        balance_in_usd = wallet.get('balanceInUSD', 0)
        deposit_in_usd = wallet.get('depositInUSD', 0)
        borrow_in_usd = wallet.get('borrowInUSD', 0)

    x1_info = {
        'balance_in_usd': balance_in_usd,
        'deposit_in_usd': deposit_in_usd,
        'borrow_in_usd': borrow_in_usd,
    }

    asset_statistics = statistics[WalletStatisticFieldConstant.total_asset]
    deposit_statistics = statistics[WalletStatisticFieldConstant.deposit]
    borrow_statistics = statistics[WalletStatisticFieldConstant.borrow]

    # x11 - Total asset
    x11 = balance_in_usd + deposit_in_usd - borrow_in_usd
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
    x1 = WalletCreditScoreWeightConstantV3.b11 * x11 + WalletCreditScoreWeightConstantV3.b12 * x12

    average_balance = get_average(list(balance_logs.values()), list(balance_logs.keys()), current_time,
                                  timestamp_chosen)
    average_borrow, time_borrow_over_threshold = get_average(list(borrow_logs.values()), list(borrow_logs.keys()),
                                                             current_time, timestamp_chosen,
                                                             threshold=borrow_statistics['mean'] / 2)
    average_deposit, time_deposit_over_threshold = get_average(list(deposit_logs.values()), list(deposit_logs.keys()),
                                                               current_time, timestamp_chosen,
                                                               threshold=deposit_statistics['mean'] / 2)

    x5_info = {}

    # x51 - Loan to value
    if average_balance <= 0:
        x51 = 0
        loan_to_balance = 0
    else:
        loan_to_balance = average_borrow / average_balance
        x51 = 850 * max(0, 1 - max(0, -1 + loan_to_balance))
        x51 = about(x51 * time_borrow_over_threshold / (current_time - timestamp_chosen))

    x5_info['average_balance'] = average_balance
    x5_info['average_deposit'] = average_deposit
    x5_info['average_borrow'] = average_borrow
    x5_info['loan_to_balance'] = loan_to_balance
    x5_info['time_borrow_over_threshold'] = time_borrow_over_threshold

    # x32 - Loan to investment
    if average_deposit <= 0:
        x52 = 0
        x5_info['loan_to_investment'] = 0
    else:
        loan_to_investment = average_borrow / average_deposit
        x52 = about(850 * max(0, 1 - max(0, -1 + loan_to_investment)))
        x5_info['loan_to_investment'] = loan_to_investment

    # x5 - Loan ratios
    x5 = about(x51 - (850 - x52))

    # x6 - Circulating asset
    x6_info = {}

    # x61 - investment to total asset ratio
    investment_to_asset = average_deposit / average_asset if average_asset > 0 else 0
    x6_info['investment_per_asset'] = investment_to_asset
    x6_info['time_deposit_over_threshold'] = time_deposit_over_threshold
    x61 = about(850 * investment_to_asset * time_deposit_over_threshold / (current_time - timestamp_chosen))
    x6 = WalletCreditScoreWeightConstantV3.b61 * x61

    info = {
        'x1': x1_info,
        'x5': x5_info,
        'x6': x6_info
    }

    if return_info:
        return x1, x5, x6, [x11, x12], [x51, x52], [x61], info
    return x1, x5, x6, [x11, x12], [x51, x52], [x61]


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


def calculate_x234(wallet, statistics, current_time=time.time(), timestamp_chosen=0, return_info=False):
    created_at = wallet.get('createdAt') or 0
    daily_transaction_amounts = remove_null({int(t): v for t, v in wallet.get('dailyTransactionAmounts', {}).items()})
    daily_frequency_of_transactions = remove_null(
        {int(t): v for t, v in wallet.get('dailyNumberOfTransactions', {}).items()})
    frequency_of_dapp_transactions = remove_null(
        {int(t): v for t, v in wallet.get('frequencyOfDappTransactions', {}).items()})
    number_of_interacted_dapps = remove_null(
        {int(t): v for t, v in wallet.get('numberOfInteractedDapps', {}).items()})
    # daily_types_of_dapps = remove_null(
    #     {int(t): v for t, v in wallet.get('typesOfDapps', {}).items()})
    number_of_liquidation = 0
    total_amount_of_liquidation = 0
    liquidation_logs = wallet.get("liquidationLogs", {}).get("liquidatedWallet")
    for wallet, liquidation in liquidation_logs.items():
        for timestamp_, value in liquidation.items():
            if int(timestamp_) <= current_time:
                number_of_liquidation += 1
                total_amount_of_liquidation += value["debtAssetInUSD"]

    # number_of_liquidation = wallet.get('numberOfLiquidation') or 0
    # total_amount_of_liquidation = wallet.get('totalValueOfLiquidation') or 0
    x2_info, x3_info, x4_info = {}, {}, {}

    # x21 - frequency of dapp transaction
    daily_frequency_of_dapp_transaction = sum_frequency(
        [v for t, v in frequency_of_dapp_transactions.items() if timestamp_chosen < t < current_time])
    if daily_frequency_of_dapp_transaction < 0:
        daily_frequency_of_dapp_transaction = 0
    frequency_statistic = statistics[WalletStatisticFieldConstant.frequency_of_dapp_transaction]
    x21 = about(frequency_statistic['coefficient_a'] * pow(daily_frequency_of_dapp_transaction,
                                                           frequency_statistic['coefficient_b']))

    # x22 - daily number of interacted dapps
    daily_number_of_interacted_dapps = sum_frequency(
        [v for t, v in number_of_interacted_dapps.items() if timestamp_chosen < t < current_time])
    if daily_number_of_interacted_dapps < 0:
        daily_number_of_interacted_dapps = 0
    frequency_statistic = statistics[WalletStatisticFieldConstant.number_of_dapps]
    x22 = about(frequency_statistic['coefficient_a'] * pow(daily_number_of_interacted_dapps,
                                                           frequency_statistic['coefficient_b']))

    # x23 - daily_types_of_dapps
    # daily_types_of_dapps = sum_frequency(
    #     [v for t, v in daily_types_of_dapps.items() if timestamp_chosen < t < current_time])
    # if daily_types_of_dapps < 0:
    #     daily_types_of_dapps = 0
    # frequency_statistic = statistics[WalletStatisticFieldConstant.types_of_dapps]
    # x23 = about(frequency_statistic['coefficient_a'] * pow(len(total_types_of_dapps),
    #                                                        frequency_statistic['coefficient_b']))
    x23 = 690

    # reputation of interacted Dapps
    x24 = 690

    x2_info['total_dapps'] = daily_number_of_interacted_dapps
    x2_info['daily_frequency_of_dapp_transaction'] = daily_frequency_of_dapp_transaction

    # x31 - transaction amount
    daily_transaction_amount = sum(
        [v for t, v in daily_transaction_amounts.items() if timestamp_chosen < t < current_time])
    if daily_transaction_amount < 0:
        daily_transaction_amount = 0
    amount_statistic = statistics[WalletStatisticFieldConstant.transaction_amount]
    x31 = about(amount_statistic['coefficient_a'] * pow(daily_transaction_amount, amount_statistic['coefficient_b']))
    x3_info['daily_transaction_amount'] = daily_transaction_amount

    # x32 - frequency of transaction
    daily_frequency_of_transaction = sum_frequency(
        [v for t, v in daily_frequency_of_transactions.items() if timestamp_chosen < t < current_time])
    if daily_frequency_of_transaction < 0:
        daily_frequency_of_transaction = 0
    frequency_statistic = statistics[WalletStatisticFieldConstant.frequency_of_transaction]
    x32 = about(frequency_statistic['coefficient_a'] * pow(daily_frequency_of_transaction,
                                                           frequency_statistic['coefficient_b']))
    x3_info['daily_frequency_of_transaction'] = daily_frequency_of_transaction

    # x41 - Age of account
    age = current_time - created_at
    age_statistic = statistics[WalletStatisticFieldConstant.age_of_account]
    if age < 0:
        age = 0

    # x41 = get_tscore_with_adjust(age, age_statistic['mean'], age_statistic['std'])
    x41 = about(age_statistic['coefficient_a'] * pow(age, age_statistic['coefficient_b']))
    x4_info['age_of_account'] = age

    # x42 - number of liquidations
    x4_info['number_of_liquidation'] = number_of_liquidation
    x42 = about(850 - 85 * number_of_liquidation)

    # x43 - total value of liquidations
    x4_info['total_amount_of_liquidation'] = total_amount_of_liquidation
    x43 = 0.85 * about(1000 - 0.1 * total_amount_of_liquidation)

    # x2 - DApp interactions
    x2 = WalletCreditScoreWeightConstantV3.b21 * x21 + WalletCreditScoreWeightConstantV3.b22 * x22 + \
         WalletCreditScoreWeightConstantV3.b23 * x23 + WalletCreditScoreWeightConstantV3.b24 * x24
    # x3 - Transactions with other wallets
    x3 = WalletCreditScoreWeightConstantV3.b31 * x31 + WalletCreditScoreWeightConstantV3.b32 * x32
    # x4 - liquidation history
    x4 = WalletCreditScoreWeightConstantV3.b41 * x41 + WalletCreditScoreWeightConstantV3.b42 * x42 + \
         WalletCreditScoreWeightConstantV3.b43 * x43

    if return_info:
        return x2, x3, x4, [x21, x22], [x31, x32], [x41, x42, x43], x2_info, x3_info, x4_info
    return x2, x3, x4, [x21, x22], [x31, x32], [x41, x42, x43]


def calculate_x7(wallet, tokens, current_time, return_info=False, history=False):
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

    x7_info = {}
    max_token_credit = 0
    for token in wallet_tokens:
        if token in tokens.keys():
            token_credit = tokens[token]
            if token_credit > max_token_credit:
                max_token_credit = token_credit
            x7_info[token] = token_credit

    x71 = about(max_token_credit)
    x72 = 0
    x7 = 0.85 * WalletCreditScoreWeightConstantV3.b71 * x71 + WalletCreditScoreWeightConstantV3.b72 * x72
    if return_info:
        return x7, [x71, x72], x7_info
    return x7, [x71, x72]


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
