from copy import deepcopy
from typing import List
from config import get_logger
from constants.constants import ChainConstant, TokenCollections

logger = get_logger(__name__)


def get_connection_elements(string):
    """
    example output for exporter_type: exporter_type@username:password@connection_url

    :param string:
    :return: username, password, connection_url
    """
    try:
        elements = string.split("@")
        auth = elements[1].split(":")
        username = auth[0]
        password = auth[1]
        connection_url = elements[2]
        return username, password, connection_url
    except Exception as e:
        logger.warning(f"get_connection_elements err {e}")
        return None, None, None


def to_bool(value):
    if type(value) == bool:
        return value

    if type(value) == str:
        if value.lower() == 'true':
            return True
        return False

    return bool(value)


def get_chain_id(chain_name, required=False, default=None):
    if chain_name is None:
        if not required:
            return default

    for _chain_id, _chain_name in zip(ChainConstant.all, ChainConstant.names):
        if chain_name.upper() == _chain_name:
            return _chain_id


def split_token(token, merged=True):
    data = {}

    _key = token['_key']
    info = deepcopy(token)

    prefix = 'merged_token' if merged else 'token'
    for key, fields in TokenCollections.mappings.items():
        value_field = fields['value']
        logs_field = fields['logs']

        if (logs_field not in info) or (value_field not in info):
            continue

        data[f'{prefix}_{key}'] = {
            '_key': _key,
            value_field: info.get(value_field),
            logs_field: info.pop(logs_field)
        }

    data[f'{prefix}s'] = info

    return data


def merge_token(data):
    token = {}
    for key, value in data.items():
        token.update(value)

    return token


def get_token_query(_key: str = None, _keys: List = None, chain_id: str = None, addresses: List = None,
                    filter_: List = None, merge=True):
    if _key is not None:
        filter_ = f"FILTER token._key == '{_key}'"
    elif _keys is not None:
        filter_ = f"FILTER token._key IN {_keys}"
    elif chain_id is not None:
        filter_ = f"FILTER token.chainId == '{chain_id}'"
    elif addresses is not None:
        filter_ = f"FILTER token.address IN {addresses}"
    elif filter_ is not None:
        filter_ = [f'FILTER {f}' for f in filter_]
        filter_ = '\n'.join(filter_)
        # filter_ = 'FILTER ' + ' AND '.join([f"{key} == '{value}'" for key, value in filter_.items()])
    else:
        filter_ = ''

    prefix = 'merged_token' if merge else 'token'
    query_filter = ''
    query_return = []
    for key, fields in TokenCollections.mappings.items():
        query_filter += f'''
            LET {key} = (
                FOR {key} IN {prefix}_{key}
                FILTER token._key == {key}._key
                RETURN {key}
            )'''

        query_return.append(f"{fields['logs']}: {key}[0].{fields['logs']}")

    query_return = ','.join(query_return)
    query_return = '{' + query_return + '}'

    query = f'''
        FOR token in {prefix}s
        {filter_}
        {query_filter}
        RETURN merge(token, {query_return})
    '''
    # with open('query.txt', 'w') as f:
    #     f.write(query)
    #     f.write('\n')

    return query


def remove_null(change_logs: dict):
    keys = list(change_logs.keys())
    for key in keys:
        if change_logs[key] is None:
            change_logs.pop(key)
    return change_logs
