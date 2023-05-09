import csv


def write_csv(data, file_path):
    if not data:
        return

    with open(file_path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)


def read_csv(file_path, key, fields=None):
    results = {}
    with open(file_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key_ = row.pop(key)
            if fields is None:
                items = {field: float(value or 0) for field, value in row.items()}
            else:
                items = {field: float(value or 0) for field, value in row.items() if field in fields}
            results[key_] = items
    return results
