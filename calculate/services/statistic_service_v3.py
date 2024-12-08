import math
import time

import numpy as np

from config import get_logger

logger = get_logger('Statistics')


def get_standardized_score_info(a):
    a = np.array(a)
    return int(a.mean()), int(a.std())


def get_standardized_score(a):
    a = np.array(a)
    return a.mean(), a.std()


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


def about(value, _min=300, _max=850):
    if value < _min:
        value = _min
    elif value > _max:
        value = _max
    return int(value)

def about_liquidate(value, _min=0, _max=850):
    if value < _min:
        value = _min
    elif value > _max:
        value = _max
    return int(value)


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
        if int(timestamp) - 30*86400 < int(t) <= int(timestamp):
            value = v
    return value

def get_list_value_with_timestamp(change_logs, timestamp, default=None):
    if default is None:
        default = []
    value = default
    for t, v in change_logs.items():
        if int(timestamp) - 30*86400 < int(t) <= int(timestamp):
            value += v
    value = list(set(value))
    return len(value)


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


def get_function_coefficients(value_low, value_high, score_low=690, score_high=850):
    temp_1 = value_high / value_low
    temp_2 = score_high / score_low
    b = math.log(temp_2, temp_1)
    a = score_low / (pow(value_low, b))
    #   print(f"a: {a}, b: {b}")
    return a, b
