import time

from calculate.services.statistic_service import calculate_average, about, get_tscore, sum_frequency
from config.constant import WalletStatisticFieldConstant, WalletCreditScoreWeightConstant


def calculate_credit_score(wallet, statistics, tokens, k, h, current_time=time.time(), return_elements=False):
    timestamp_chosen = current_time - 86400 * k
    x1, x3, x4, x1_s, x3_s, x4_s = calculate_x134(wallet, statistics, current_time, timestamp_chosen=timestamp_chosen, h=h)
    x2, x2_s = calculate_x2(wallet, statistics, current_time)
    x5 = calculate_x5(wallet, tokens)
    credit_score = WalletCreditScoreWeightConstant.a1 * x1 + WalletCreditScoreWeightConstant.a2 * x2 + WalletCreditScoreWeightConstant.a3 * x3 + WalletCreditScoreWeightConstant.a4 * x4 + WalletCreditScoreWeightConstant.a5 * x5
    if return_elements:
        return int(credit_score), [x1_s, x2_s, x3_s, x4_s, x5]
    return int(credit_score)


def calculate_credit_score_with_info_return(wallet, statistics, tokens, k, h, current_time=time.time()):
    timestamp_chosen = current_time - 86400 * k
    x1, x3, x4, x1_s, x3_s, x4_s, info = calculate_x134(wallet, statistics, current_time, timestamp_chosen=timestamp_chosen, h=h, return_info=True)
    x2, x2_s, x2_info = calculate_x2(wallet, statistics, current_time, return_info=True)
    x5, x5_info = calculate_x5(wallet, tokens, return_info=True)

    info.update({
        'x2': x2_info,
        'x5': x5_info,
    })
    info = dict(sorted(info.items(), key=lambda x: x[0]))
    credit_score = WalletCreditScoreWeightConstant.a1 * x1 + WalletCreditScoreWeightConstant.a2 * x2 + WalletCreditScoreWeightConstant.a3 * x3 + WalletCreditScoreWeightConstant.a4 * x4 + WalletCreditScoreWeightConstant.a5 * x5
    return int(credit_score), [x1_s, x2_s, x3_s, x4_s, x5], info


def calculate_x134(wallet, statistics, current_time=time.time(), timestamp_chosen=0, h=10, return_info=False):
    # Get wallet asset info
    balanceInUSD = wallet.get('balanceInUSD', 0)
    depositInUSD = wallet.get('depositInUSD', 0)
    borrowInUSD = wallet.get('borrowInUSD', 0)

    balanceChangeLogTimestamps = list(wallet.get('balanceChangeLogs', {}).keys())
    balanceChangeLogTimestamps = [int(i) for i in balanceChangeLogTimestamps]
    balanceChangeLogValues = list(wallet.get('balanceChangeLogs', {}).values())

    depositChangeLogTimestamps = list(wallet.get('depositChangeLogs', {}).keys())
    depositChangeLogTimestamps = [int(i) for i in depositChangeLogTimestamps]
    depositChangeLogValues = list(wallet.get('depositChangeLogs', {}).values())

    borrowChangeLogTimestamps = list(wallet.get('borrowChangeLogs', {}).keys())
    borrowChangeLogTimestamps = [int(i) for i in borrowChangeLogTimestamps]
    borrowChangeLogValues = list(wallet.get('borrowChangeLogs', {}).values())

    x1_info = {
        'balance_in_usd': balanceInUSD,
        'deposit_in_usd': depositInUSD,
        'borrow_in_usd': borrowInUSD,
    }

    # x11 - total asset
    x11 = balanceInUSD + depositInUSD - borrowInUSD
    x1_info['total_current_asset'] = x11
    if x11 < 1000:
        x11 = 0
    else:
        x11 = about(0.1 * x11)

    # x12 - total asset average
    balance_average = calculate_average(balanceChangeLogValues, balanceChangeLogTimestamps, current_time, timestamp_chosen)
    if balance_average == 0:
        balance_average = balanceInUSD
    x1_info['balance_average'] = balance_average

    loan_average = calculate_average(borrowChangeLogValues, borrowChangeLogTimestamps, current_time, timestamp_chosen)
    if loan_average == 0:
        loan_average = borrowInUSD
    x1_info['borrow_average'] = loan_average

    deposit_average = calculate_average(depositChangeLogValues, depositChangeLogTimestamps, current_time, timestamp_chosen)
    if deposit_average == 0:
        deposit_average = depositInUSD
    x1_info['deposit_average'] = deposit_average

    total_asset_average = balance_average + deposit_average - loan_average
    x1_info['total_asset_average'] = total_asset_average

    asset_statistic = statistics[WalletStatisticFieldConstant.total_asset]
    if total_asset_average <= 0:
        x12 = 0
    else:
        x12 = get_tscore(total_asset_average, asset_statistic['mean'], asset_statistic['std'], log=True)

    # x1 - asset
    x1 = WalletCreditScoreWeightConstant.b11 * x11 + WalletCreditScoreWeightConstant.b12 * x12

    # x3 - loan ratio
    x3_info = {}
    if balance_average <= 0:
        x31 = 0
        x3_info['loan_per_balance'] = 0
    else:
        loan_per_balance = loan_average / balance_average
        x31 = about(1000 * (1 - min(1, loan_per_balance)))
        x3_info['loan_per_balance'] = loan_per_balance

    if deposit_average <= 0:
        x32 = 0
        x3_info['loan_per_deposit'] = 0
    else:
        loan_per_deposit = loan_average / deposit_average
        x32 = about(1000 * (1 - min(1, loan_per_deposit)))
        x3_info['loan_per_deposit'] = loan_per_deposit

    x3 = WalletCreditScoreWeightConstant.b31 * x31 + WalletCreditScoreWeightConstant.b32 * x32

    # x4 - Circulating asset
    x4_info = {}

    # x41 - investment to total asset ratio
    investment_per_asset = deposit_average / total_asset_average if total_asset_average > 0 else 0
    x4_info['investment_per_asset'] = investment_per_asset
    x41 = about(1000 * investment_per_asset)

    # x42 - Return on investment ROI
    if not depositChangeLogTimestamps:
        x42 = 0
        x4_info['ROI'] = 0
    else:
        time_limit = 86400 * h
        return_on_investment = 0
        for i in range(len(depositChangeLogTimestamps) - 1):
            if depositChangeLogTimestamps[i] > (current_time - time_limit):
                continue
            if depositChangeLogValues[i] != depositChangeLogTimestamps[i + 1]:
                if depositChangeLogTimestamps[i] in balanceChangeLogTimestamps:
                    j = balanceChangeLogTimestamps.index(depositChangeLogTimestamps[i])
                    if j < len(balanceChangeLogTimestamps) - 1:
                        if balanceChangeLogTimestamps[j + 1] == depositChangeLogTimestamps[i + 1]:
                            d0 = depositChangeLogValues[i]
                            d1 = depositChangeLogValues[i + 1]
                            b0 = balanceChangeLogValues[j]
                            b1 = balanceChangeLogValues[j + 1]
                            period_of_time = depositChangeLogTimestamps[i + 1] - depositChangeLogTimestamps[i]
                            if b1 == b0:
                                continue
                            profit = b1 + d1 - b0 - d0
                            if d0 != 0:
                                return_on_investment_temp = (profit / d0) * (period_of_time / time_limit)
                                return_on_investment += return_on_investment_temp
        x4_info['ROI'] = return_on_investment
        x42 = about((return_on_investment * 365 * 1000) / (h * 0.15))
    x4 = WalletCreditScoreWeightConstant.b41 * x41 + WalletCreditScoreWeightConstant.b42 * x42

    info = {
        'x1': x1_info,
        'x3': x3_info,
        'x4': x4_info
    }
    if return_info:
        return x1, x3, x4, [x11, x12], [x31, x32], [x41, x42], info
    return x1, x3, x4, [x11, x12], [x31, x32], [x41, x42]


def calculate_x2(wallet, statistics, current_time=time.time(), return_info=False):
    created_at = wallet.get('createdAt', 0)
    daily_transaction_amounts = list(wallet.get('dailyTransactionAmounts', {}).values())
    daily_frequency_of_transactions = list(wallet.get('dailyNumberOfTransactions', {}).values())
    number_of_liquidation = wallet.get('numberOfLiquidation', 0)
    total_amount_of_liquidation = wallet.get('totalValueOfLiquidation', 0)

    x2_info = {}

    # x21 - age of account
    if created_at == 0:
        x21 = 0
        x2_info['age_of_account'] = 0
    else:
        age = current_time - created_at
        age_statistic = statistics[WalletStatisticFieldConstant.age_of_account]
        x21 = get_tscore(age, age_statistic['mean'], age_statistic['std'])
        x2_info['age_of_account'] = age

    # x22 - transaction amount
    if not daily_transaction_amounts:
        x22 = 0
        x2_info['daily_transaction_amount'] = 0
    else:
        daily_transaction_amount = sum(daily_transaction_amounts)
        amount_statistic = statistics[WalletStatisticFieldConstant.transaction_amount]
        x22 = get_tscore(daily_transaction_amount, amount_statistic['mean'], amount_statistic['std'], log=True)
        x2_info['daily_transaction_amount'] = daily_transaction_amount

    # x23 - frequency of transaction
    if not daily_frequency_of_transactions:
        x23 = 0
        x2_info['daily_frequency_of_transaction'] = 0
    else:
        daily_frequency_of_transaction = sum_frequency(daily_frequency_of_transactions)
        frequency_statistic = statistics[WalletStatisticFieldConstant.frequency_of_transaction]
        x23 = get_tscore(daily_frequency_of_transaction, frequency_statistic['mean'], frequency_statistic['std'])
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


def calculate_x5(wallet, tokens, return_info=False):
    wallet_tokens = list(wallet.get('tokens', {}).keys())

    x5_info = {}
    max_token_credit = 0
    for token in wallet_tokens:
        if token in tokens.keys():
            token_credit = tokens[token]
            if token_credit > max_token_credit:
                max_token_credit = token_credit
            x5_info[token] = token_credit

    if return_info:
        return about(max_token_credit), x5_info
    return about(max_token_credit)
