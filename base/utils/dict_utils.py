def get_first_value(dict={}, *keys):
    for key in keys:
        if dict.get(key):
            return dict.get(key)
    return None
