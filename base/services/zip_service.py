import logging


def two_list_to_dict(keys_list, values_list):
    """
    example:

        keys_list = ["a", "b"]
        values_list = [1, 2]
        zip_iterator = zip(keys_list, values_list)

        a_dictionary = dict(zip_iterator)
        print(a_dictionary)

    :param keys_list:
    :param values_list:
    :return: dict()
    """
    zip_iterator = zip(keys_list, values_list)

    return dict(zip_iterator)


def dict_to_two_list(mapper):
    """
    example
        tel = {'jack': 4098, 'sape': 4139}
        keys = list(tel.keys())
        values = list(tel.values())

        print(keys)
        print(values)

    :param mapper:
    :return: keys, values : list(),list()
    """
    try:
        keys = list(mapper.keys())
        values = list(mapper.values())
    except Exception as e:
        logging.warning(f"err when conver dict to list {e}")
        return [],[]
    return keys, values
