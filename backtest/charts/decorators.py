import json

import numpy as np


class Decorator:
    @staticmethod
    def make_timestamp_dict(timestamps):
        result = {}
        for item in range(len(timestamps)):
            if item == len(timestamps) - 1: continue
            result[timestamps[item + 1]] = timestamps[item]
        return result

    @staticmethod
    def make_object_data(timestamps):
        result = {}
        for item in range(len(timestamps)):
            result[timestamps[item]] = 0
        return result

    @staticmethod
    def make_check_data(timestamps):
        result = {}
        for item in range(len(timestamps)):
            result[timestamps[item]] = False
        return result

    @staticmethod
    def check_timestamp(timestamp, timestamps):
        tmp = timestamps[0]
        for item in timestamps:
            if int(timestamp) <= item:
                return tmp
            tmp = item
        return tmp

    @staticmethod
    def reduce_event(data, label='debt'):
        data = dict(sorted(data.items(), key=lambda x: x[0]))
        tmp = None
        result = {}
        for key in data:
            result[key] = data[key]
            if not tmp:
                tmp = key
                continue
            if int(key) - int(tmp) < 86400 and data[key][f"{label}Asset"] == data[tmp][f"{label}Asset"]:
                result[key][f"{label}Amount"] += result[tmp][f"{label}Amount"]
                result[key][f"{label}AssetInUSD"] += result[tmp][f"{label}AssetInUSD"]
                del result[tmp]
            tmp = key
        return result

    @staticmethod
    def func(pct, allvals):
        absolute = int(np.round(pct / 100. * sum(allvals)))
        return f"{pct:.1f}%\n({'{:,}'.format(absolute)} USD)"

    @staticmethod
    def write_file(file, data):
        try:
            with open(file, 'w') as f:
                f.write(json.dumps(data))
        except Exception as e:
            raise e
