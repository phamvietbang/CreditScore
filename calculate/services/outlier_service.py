def find_median(sorted_list):
    list_size = len(sorted_list)
    if list_size % 2 == 0:
        x = int(list_size / 2) - 1
        y = int(list_size / 2)
        median = (sorted_list[x] + sorted_list[y]) / 2
        indices = [x, y]
    else:
        x = int(list_size / 2)
        median = sorted_list[x]
        indices = [x]
    return median, indices


def get_quartile(sorted_list):
    median, median_indices = find_median(sorted_list)
    Q1, Q1_indices = find_median(sorted_list[:median_indices[0]])
    Q3, Q3_indices = find_median(sorted_list[median_indices[-1] + 1:])
    return [Q1, median, Q3]


def get_index(value, sorted_list, start=0, end=None):
    if end is None:
        end = len(sorted_list)

    if end == start or sorted_list[-1] < value:
        return end

    mid = int((start + end) / 2)
    if sorted_list[mid] == value:
        return mid
    elif sorted_list[mid] > value:
        return get_index(value, sorted_list, start=start, end=mid)
    else:
        return get_index(value, sorted_list, start=mid + 1, end=end)


def ignore_outliers(array, lower=True, upper=True):
    sorted_list = sorted(array)

    Q1, median, Q3 = get_quartile(sorted_list)
    IQR = Q3 - Q1
    upper_limit = Q3 + 1.5 * IQR
    lower_limit = Q1 - 1.5 * IQR

    lower_idx = get_index(lower_limit, sorted_list) if lower else 0
    upper_idx = get_index(upper_limit, sorted_list) if upper else len(sorted_list)
    sorted_list = sorted_list[lower_idx:upper_idx]
    return sorted_list
