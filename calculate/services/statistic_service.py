import math
import time

import numpy as np
from scipy.stats.stats import _contains_nan

from config import get_logger
from calculate.services.outlier_service import ignore_outliers

logger = get_logger('Statistics')


def get_standardized_score_info(a, axis=0, ddof=0, nan_policy='propagate'):
    a = np.asanyarray(a)

    contains_nan, nan_policy = _contains_nan(a, nan_policy)

    if contains_nan and nan_policy == 'omit':
        mns = np.nanmean(a=a, axis=axis, keepdims=True)
        sstd = np.nanstd(a=a, axis=axis, ddof=ddof, keepdims=True)
    else:
        mns = a.mean(axis=axis, keepdims=True)
        sstd = a.std(axis=axis, ddof=ddof, keepdims=True)

    return int(mns[0]), int(sstd[0])


def get_standardized_score(a, axis=0, ddof=0, nan_policy='propagate'):
    a = np.asanyarray(a)

    contains_nan, nan_policy = _contains_nan(a, nan_policy)

    if contains_nan and nan_policy == 'omit':
        mns = np.nanmean(a=a, axis=axis, keepdims=True)
        sstd = np.nanstd(a=a, axis=axis, ddof=ddof, keepdims=True)
    else:
        mns = a.mean(axis=axis, keepdims=True)
        sstd = a.std(axis=axis, ddof=ddof, keepdims=True)

    return mns[0], sstd[0]


def get_tscore(value, mean, std, log=False):
    if std == 0:
        return about(mean)

    if log:
        if value <= 0:
            return 0
        value = math.log(value)

    zscore = (value - mean) / std
    tscore = 100 * zscore + 500
    return about(tscore)


def get_function_score(value, coefficient_a, coefficient_b):
    if value < 0:
        return 0
    score = coefficient_a * math.pow(value, coefficient_b)
    return about(score)


def get_tscore_with_adjust(value, mean, std):
    if std == 0:
        return about(100 * mean / std)

    tscore = 100 * value / std
    return about(tscore)


def about(value, _min=0, _max=1000):
    if value < _min:
        value = _min
    elif value > _max:
        value = _max
    return value


def get_mean_std(a):
    a = np.array(a)
    return a.mean(), a.std()


def get_median(array, _sorted=False):
    if not _sorted:
        array = sorted(array)
    length = len(array)
    idx = int(length / 2)
    if length % 2:
        return array[idx]
    else:
        return (array[idx - 1] + array[idx]) / 2


def get_value_with_timestamp(change_logs, timestamp, default=0):
    value = default
    for t, v in change_logs.items():
        if t <= timestamp:
            value = v
    return value


def get_logs_in_time(change_logs, start_time=0, end_time=int(time.time())):
    logs = {}
    for t, v in change_logs.items():
        if start_time <= t <= end_time:
            logs[t] = v
    return logs


def get_average(values, timestamps, current_time, timestamp_chosen=0, threshold=None):
    if not values:
        average = 0
        total_time = 0
    else:
        out_value = values[0]
        out_idx = None
        out_time_idx = None
        total_time = 0
        average = 0
        for idx in range(0, len(timestamps) - 1):
            if timestamps[idx] < timestamp_chosen:
                out_value = values[idx]
                out_idx = idx
            elif timestamps[idx] > current_time:
                out_time_idx = idx
                break
            else:
                average += values[idx] * (timestamps[idx + 1] - timestamps[idx])
                if (threshold is not None) and (values[idx] > threshold):
                    total_time += timestamps[idx + 1] - timestamps[idx]

        if timestamps[-1] < timestamp_chosen:
            out_value = values[-1]
            out_idx = len(timestamps) - 1
        elif (out_time_idx is None) and (timestamps[-1] > current_time):
            out_time_idx = len(timestamps) - 1

        if out_idx is not None:
            next_timestamp = timestamps[out_idx + 1] if out_idx < len(timestamps) - 1 else timestamp_chosen
            average += values[out_idx] * (next_timestamp - timestamp_chosen)
            if (threshold is not None) and (out_value > threshold):
                total_time += next_timestamp - timestamp_chosen

        if out_time_idx is None:
            last_timestamp = max(timestamps[-1], timestamp_chosen)
            average += values[-1] * (current_time - last_timestamp)
            if (threshold is not None) and (values[-1] > threshold):
                total_time += current_time - last_timestamp
        else:
            if out_time_idx > 0:
                average -= values[out_time_idx - 1] * (timestamps[out_time_idx] - current_time)
                if (threshold is not None) and (values[out_time_idx - 1] > threshold):
                    total_time -= timestamps[out_time_idx] - current_time
        average /= current_time - timestamp_chosen

    if threshold is not None:
        return average, total_time
    return average


def get_return_on_investment(balance_logs, deposit_logs, current_time, h=10):
    balanceChangeLogTimestamps = list(balance_logs.keys())
    balanceChangeLogValues = list(balance_logs.values())
    depositChangeLogTimestamps = list(deposit_logs.keys())
    depositChangeLogValues = list(deposit_logs.values())
    if not depositChangeLogTimestamps:
        return 0

    time_limit = 86400 * h
    return_on_investment = 0
    for i in range(len(depositChangeLogTimestamps) - 1):
        if depositChangeLogTimestamps[i] > (current_time - time_limit):
            continue
        if depositChangeLogTimestamps[i] != depositChangeLogTimestamps[i + 1]:
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

    return return_on_investment


def calculate_average(values, timestamps, time_current, timestamp_chosen=0.0):
    total_time = time_current - timestamp_chosen
    if not values:
        return 0

    if len(values) == 1:
        if time_current == timestamps[0]:
            return 0
        else:
            real_time = min(time_current - timestamps[0], total_time)
            return values[0] * real_time / total_time

    d = dict(zip(timestamps, values))
    dictionary_items = d.items()
    sorted_items = sorted(dictionary_items)
    timestamps = []
    values = []

    first_value = sorted_items[0][1] if sorted_items else 0
    for idx, item in enumerate(sorted_items):
        if item[0] > timestamp_chosen:
            timestamps.append(item[0])
            values.append(item[1])
        else:
            first_value = item[1]

    if sorted_items and sorted_items[0][0] < timestamp_chosen:
        timestamps.insert(0, timestamp_chosen)
        values.insert(0, first_value)

    if not timestamps:
        return 0

    _sum = 0
    for i in range(len(values) - 1):
        _sum += values[i] * (timestamps[i + 1] - timestamps[i])
    _sum += values[-1] * (time_current - timestamps[-1])
    # total_time = time_current - timestamps[0]
    average = _sum / total_time if total_time != 0 else 0
    return average


def sum_frequency(array):
    if type(array) is not list:
        return array

    _sum = 0
    for cnt in array:
        _sum += cnt if type(cnt) == int else 1
    return _sum


def logarit(array):
    return np.log([a for a in array if a > 1e-18])


def get_statistics(wallets, k=30, current_time=time.time()):
    timestamp_chosen = current_time - k * 86400
    created_ats = []
    daily_frequency_of_transactions = []
    daily_transaction_amounts = []
    total_assets = []

    cnt = 0
    start_time = time.time()
    for wallet in wallets:
        # Get age info x21
        created_at = wallet.get('createdAt', 0)
        if created_at > 0:
            created_ats.append(created_at)

        # Get Daily Transaction Amount x22
        daily_transaction_amount = wallet.get('dailyTransactionAmounts')
        if daily_transaction_amount:
            daily_transaction_amounts.append(sum(daily_transaction_amount))

        # Get Daily Frequent Transaction x23
        daily_frequency_of_transaction = wallet.get('dailyFrequencyOfTransactions')
        if daily_frequency_of_transaction:
            daily_frequency_of_transactions.append(sum_frequency(daily_frequency_of_transaction))

        # Get total asset info
        balanceInUSD = wallet.get('balanceInUSD', 0)
        depositInUSD = wallet.get('depositInUSD', 0)
        borrowInUSD = wallet.get('borrowInUSD', 0)
        balanceChangeLogTimestamps = wallet.get('balanceChangeLogTimestamps', 0)
        balanceChangeLogValues = wallet.get('balanceChangeLogValues', 0)
        depositChangeLogTimestamps = wallet.get('depositChangeLogTimestamps', 0)
        depositChangeLogValues = wallet.get('depositChangeLogValues', 0)
        borrowChangeLogTimestamps = wallet.get('borrowChangeLogTimestamps', 0)
        borrowChangeLogValues = wallet.get('borrowChangeLogValues', 0)

        balance_average = calculate_average(balanceChangeLogValues, balanceChangeLogTimestamps, current_time,
                                            timestamp_chosen)
        if balance_average == 0:
            balance_average = balanceInUSD

        loan_average = calculate_average(borrowChangeLogValues, borrowChangeLogTimestamps, current_time,
                                         timestamp_chosen)
        if loan_average == 0:
            loan_average = borrowInUSD

        deposit_average = calculate_average(depositChangeLogValues, depositChangeLogTimestamps, current_time,
                                            timestamp_chosen)
        if deposit_average == 0:
            deposit_average = depositInUSD

        total_asset_average = balance_average + deposit_average - loan_average
        if total_asset_average > 0:
            total_assets.append(total_asset_average)

        cnt += 1
        if cnt % 50000 == 0:
            logger.info(f'Add {cnt} wallets after {time.time() - start_time} seconds')

    timestamps = np.array(created_ats)
    ages = current_time - timestamps

    age_mean, age_std = get_standardized_score_info(ignore_outliers(ages.tolist(), lower=False))
    amount_mean, amount_std = get_standardized_score_info(logarit(daily_transaction_amounts))
    frequency_mean, frequency_std = get_standardized_score_info(
        ignore_outliers(daily_frequency_of_transactions, lower=False))
    asset_mean, asset_std = get_standardized_score_info(logarit(total_assets))
    return [
        {'variable': 'total_asset', 'mean': asset_mean, 'std': asset_std},
        {'variable': 'age_of_account', 'mean': age_mean, 'std': age_std},
        {'variable': 'transaction_amount', 'mean': amount_mean, 'std': amount_std},
        {'variable': 'frequency_of_transaction', 'mean': frequency_mean, 'std': frequency_std},
    ]


def get_statistic_with_less_ram(graph, k=30, current_time=time.time(), chain_id=None, batch_size=40000):
    timestamp_chosen = current_time - k * 86400

    asset_mean, asset_std = get_avg_asset_statistic(graph, current_time, timestamp_chosen, chain_id=chain_id,
                                                    batch_size=batch_size)
    age_mean, age_std = get_age_statistic(graph, current_time, chain_id=chain_id, batch_size=batch_size)
    amount_mean, amount_std = get_amount_statistic(graph, chain_id=chain_id, batch_size=batch_size)
    frequency_mean, frequency_std = get_frequency_statistic(graph, chain_id=chain_id, batch_size=batch_size)

    return [
        {'variable': 'total_asset', 'mean': asset_mean, 'std': asset_std},
        {'variable': 'age_of_account', 'mean': age_mean, 'std': age_std},
        {'variable': 'transaction_amount', 'mean': amount_mean, 'std': amount_std},
        {'variable': 'frequency_of_transaction', 'mean': frequency_mean, 'std': frequency_std},
    ]


def get_avg_asset_statistic(graph, current_time, timestamp_chosen, chain_id=None, batch_size=100000):
    start_time = time.time()

    wallet_balances = {}
    cnt = 0
    while True:
        try:
            try:
                balances = graph.get_asset_change_logs_('balance', chain_id=chain_id, skip=cnt, limit=batch_size)
            except IndexError:
                tmp_batch_size = int(batch_size / 2)
                tmp1 = graph.get_asset_change_logs_('balance', chain_id=chain_id, skip=cnt, limit=tmp_batch_size)
                tmp2 = graph.get_asset_change_logs_('balance', chain_id=chain_id, skip=cnt + tmp_batch_size,
                                                    limit=batch_size - tmp_batch_size)
                balances = tmp1 + tmp2
                logger.warning(f'Decrease batch size from {batch_size} to {tmp_batch_size} in turn')

            for w in balances:
                avg = calculate_average(w['values'], w['timestamps'], current_time, timestamp_chosen)
                if avg <= 0:
                    avg = w['usd']
                wallet_balances[w['address']] = avg

            cnt += len(balances)
            logger.info(f'Load {cnt} wallets balances take {time.time() - start_time} seconds')
            if len(balances) < batch_size:
                break
        except IndexError as err:
            cnt += batch_size
            logger.exception(err)

    wallet_deposits = {}
    cnt = 0
    while True:
        try:
            deposits = graph.get_asset_change_logs_('deposit', chain_id=chain_id, skip=cnt, limit=batch_size)
            for w in deposits:
                avg = calculate_average(w['values'], w['timestamps'], current_time, timestamp_chosen)
                if avg <= 0:
                    avg = w['usd']
                wallet_deposits[w['address']] = avg

            cnt += len(deposits)
            logger.info(f'Load {cnt} wallets deposits take {time.time() - start_time} seconds')
            if len(deposits) < batch_size:
                break
        except IndexError as err:
            cnt += batch_size
            logger.exception(err)

    wallet_borrows = {}
    cnt = 0
    while True:
        try:
            borrows = graph.get_asset_change_logs_('borrow', chain_id=chain_id, skip=cnt, limit=batch_size)
            for w in borrows:
                avg = calculate_average(w['values'], w['timestamps'], current_time, timestamp_chosen)
                if avg <= 0:
                    avg = w['usd']
                wallet_borrows[w['address']] = avg

            cnt += len(borrows)
            logger.info(f'Load {cnt} wallets borrows take {time.time() - start_time} seconds')
            if len(borrows) < batch_size:
                break
        except IndexError as err:
            cnt += batch_size
            logger.exception(err)

    assets = []
    for w in wallet_balances:
        avg_asset = wallet_balances[w] + wallet_deposits.get(w, 0) - wallet_borrows.get(w, 0)
        if avg_asset > 0:
            assets.append(avg_asset)
    mean, std = get_standardized_score_info(logarit(assets))
    logger.info(f'Statistic asset took {time.time() - start_time} seconds')
    return mean, std


def get_age_statistic(graph, current_time, chain_id=None, batch_size=100000):
    start_time = time.time()
    ages = []
    cnt = 0
    while True:
        try:
            created_ats = graph.get_wallet_statistic_field_('createdAt', chain_id=chain_id, skip=cnt, limit=batch_size)
            for created_at in created_ats:
                if created_at:
                    age = current_time - created_at
                    ages.append(age)
            cnt += len(created_ats)
            logger.info(f'Load {cnt} wallets ages take {time.time() - start_time} seconds')
            if len(created_ats) < batch_size:
                break
        except IndexError as err:
            cnt += batch_size
            logger.exception(err)

    mean, std = get_standardized_score_info(ignore_outliers(ages, lower=False))
    logger.info(f'Statistic age took {time.time() - start_time} seconds')
    return mean, std


def get_amount_statistic(graph, chain_id=None, batch_size=100000):
    start_time = time.time()
    amounts = []
    cnt = 0
    while True:
        try:
            tx_amounts = graph.get_wallet_statistic_field_('dailyTransactionAmounts', chain_id=chain_id, skip=cnt,
                                                           limit=batch_size)
            tx_amounts = [list(item.values()) if item != {} else [0] for item in tx_amounts]

            for tx_amount in tx_amounts:
                amount = sum(tx_amount) if tx_amount else 0
                if amount > 0:
                    amounts.append(amount)
            cnt += len(tx_amounts)
            logger.info(f'Load {cnt} wallets transaction amounts take {time.time() - start_time} seconds')
            if len(tx_amounts) < batch_size:
                break
        except IndexError as err:
            cnt += batch_size
            logger.exception(err)

    mean, std = get_standardized_score_info(logarit(amounts))
    logger.info(f'Statistic transaction amount took {time.time() - start_time} seconds')
    return mean, std


def get_frequency_statistic(graph, chain_id=None, batch_size=100000):
    start_time = time.time()
    frequencies = []
    cnt = 0
    while True:
        try:
            tx_frequencies = graph.get_wallet_statistic_field_('dailyNumberOfTransactions', chain_id=chain_id,
                                                               skip=cnt, limit=batch_size)
            tx_frequencies = [list(item.values()) if item != {} else [0] for item in tx_frequencies]

            for tx_frequency in tx_frequencies:
                frequency = sum(tx_frequency) if tx_frequency else 0
                if frequency > 0:
                    frequencies.append(frequency)
            cnt += len(tx_frequencies)
            logger.info(f'Load {cnt} wallets frequency of transactions take {time.time() - start_time} seconds')
            if len(tx_frequencies) < batch_size:
                break
        except IndexError as err:
            cnt += batch_size
            logger.exception(err)

    mean, std = get_standardized_score_info(ignore_outliers(frequencies, lower=False))
    logger.info(f'Statistic transaction frequency took {time.time() - start_time} seconds')
    return mean, std


def get_function_coefficients(value_low, value_high, score_low=500, score_high=1000):
    temp_1 = value_high / value_low
    temp_2 = score_high / score_low
    b = math.log(temp_2, temp_1)
    a = score_low / (pow(value_low, b))
    #   print(f"a: {a}, b: {b}")
    return a, b
