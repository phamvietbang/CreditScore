import math

from calculate.token_credit_score import round_timestamp


def get_histogram(values, max_value, pin_width, out_range=True):
    histogram = {}
    for pin in range(0, int(max_value) + 1, pin_width):
        histogram[pin] = 0

    if out_range:
        histogram[max_value + 1] = 0
        for value in values:
            if value > max_value:
                histogram[max_value + 1] += 1
            else:
                pin = int(value / pin_width) * pin_width
                histogram[pin] += 1
    else:
        for value in values:
            pin = int(value / pin_width) * pin_width
            histogram[pin] += 1
    return histogram


def get_frequency(values, max_value):
    frequency = {}
    for pin in range(0, max_value + 1):
        frequency[pin] = 0

    for value in values:
        frequency[value] += 1
    return frequency


def get_histogram_with_range(values, ranges):
    histogram = {0: 0}
    histogram.update({x: 0 for x in ranges})
    for value in values:
        for x in histogram:
            if value >= x:
                histogram[x] += 1
                break
    return histogram


def get_histogram_with_log(values, max_value, pin_width, out_range=True):
    histogram = {0: 0}
    log_max_value = int(math.log10(max_value))
    for pin in range(0, log_max_value + 1, pin_width):
        histogram[int(math.pow(10, pin))] = 0

    if out_range:
        histogram[int(max_value) + 1] = 0
        for value in values:
            if value <= 1:
                pin = 0
            elif value > max_value:
                pin = int(max_value) + 1
            else:
                pin = int(math.log10(value) / pin_width) * pin_width
                pin = int(math.pow(10, pin))
            histogram[pin] += 1
    else:
        for value in values:
            if value <= 1:
                pin = 0
            else:
                pin = int(math.log10(value) / pin_width) * pin_width
                pin = int(math.pow(10, pin))
            histogram[pin] += 1

    if max_value > 1:
        histogram[0] += histogram[1]
        del histogram[1]

    return histogram


def get_histogram_by_ranges(values, ranges):
    results = [0] * len(ranges)
    for value in values:
        for idx, r in enumerate(ranges):
            if r[0] <= value <= r[1]:
                results[idx] += 1
                break
    return results


def get_histogram_by_ranges_and_frequency(frequency, ranges):
    results = [0] * len(ranges)
    for score, freq in frequency.items():
        for idx, r in enumerate(ranges):
            if r[0] <= score <= r[1]:
                results[idx] += freq
                break
    return results


def get_values_by_frequency(change_logs, start_time=30*86400, round_time=3600, max_value=False):
    results = {}
    last_timestamp = 0
    for t, v in change_logs.items():
        if t > start_time:
            if round_timestamp(t, round_time) != round_timestamp(last_timestamp, round_time) or (max_value and (t in results) and v > results[t]):
                results[t] = v
                last_timestamp = t

    return results
