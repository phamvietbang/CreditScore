import re


def update_scores_properties(wallet, credit_score, elements, current_time, multichain=False, merged=False):
    change_logs = {int(current_time): credit_score}
    elements_change_logs = []
    for idx in range(len(elements)):
        elements_change_logs.append({int(current_time): elements[idx]})

    updated = {
        # 'address': wallet['address'],
        'creditScore': credit_score,
        'creditScoreChangeLogs': change_logs,
        'creditScorex1': elements[0],
        'creditScorex2': elements[1],
        'creditScorex3': elements[2],
        'creditScorex4': elements[3],
        'creditScorex5': elements[4],
        'creditScorex1ChangeLogs': elements_change_logs[0],
        'creditScorex2ChangeLogs': elements_change_logs[1],
        'creditScorex3ChangeLogs': elements_change_logs[2],
        'creditScorex4ChangeLogs': elements_change_logs[3],
        'creditScorex5ChangeLogs': elements_change_logs[4],
    }
    if merged:
        updated['mergedWalletId'] = wallet['mergedWalletId']
    else:
        updated['address'] = wallet['address']
        if not multichain:
            updated['chainId'] = wallet['chainId']
    return updated


def update_scores_history(base, updated):
    for field, value in updated.items():
        if type(value) != dict:
            base[field] = value
        else:
            base_value = base.get(field, {})
            base_value.update(value)
            base[field] = base_value
    return base


def convert_data(wallets):
    results = []
    for wallet in wallets:
        results.append({
            'Wallet': wallet['address'],
            'Credit Score': wallet['creditScore'],
            'x11': wallet['creditScorex1'][0],
            'x12': wallet['creditScorex1'][1],
            'x21': wallet['creditScorex2'][0],
            'x22': wallet['creditScorex2'][1],
            'x23': wallet['creditScorex2'][2],
            'x24': wallet['creditScorex2'][3],
            'x25': wallet['creditScorex2'][4],
            'x31': wallet['creditScorex3'][0],
            'x32': wallet['creditScorex3'][1],
            'x41': wallet['creditScorex4'][0],
            'x42': wallet['creditScorex4'][1],
            'x51': wallet['creditScorex5']
        })
    return results


def convert_tokens(tokens):
    tokens_ = []
    for t in tokens:
        token_address = re.findall(r'0x[0-9a-f]*', t)[1]
        if token_address != '0x':
            tokens_.append(token_address)
    return tokens_


def convert_tokens1(tokens):
    tokens_ = {}
    for t in tokens:
        token_address = re.findall(r'0x[0-9a-f]*', t)[1]
        if token_address != '0x':
            tokens_[token_address] = tokens[t]
    return tokens_
