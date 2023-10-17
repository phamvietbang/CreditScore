class TimeConstants:
    A_MINUTE = 60
    MINUTES_5 = 300
    MINUTES_15 = 900
    A_HOUR = 3600
    A_DAY = 86400
    DAYS_7 = 7 * A_DAY
    DAYS_30 = 30 * A_DAY
    A_YEAR = 365 * A_DAY


class Chain:
    bsc = "bsc"
    ftm = "ftm"
    eth = "ethereum"
    polygon = 'polygon'
    arbitrum = 'arbitrum'
    optimism = 'optimism'
    avalanche = 'avalanche'
    mapping = {
        bsc: "0x38",
        ftm: "0xfa",
        eth: "0x1",
        polygon: "0x89",
        arbitrum: "0xa4b1",
        optimism: "0xa",
        avalanche: "0xa86a"
    }


class Amount:
    liquidated_collateral_amount_in_usd = "liquidated_collateral_amount_in_usd"
    debt_to_cover_in_usd = "debt_to_cover_in_usd"
    liquidated_collateral_amount = "liquidated_collateral_amount"
    debt_to_cover = "debt_to_cover"
    all = [liquidated_collateral_amount_in_usd, debt_to_cover_in_usd]
    mapping = {
        liquidated_collateral_amount_in_usd: liquidated_collateral_amount,
        debt_to_cover_in_usd: debt_to_cover
    }
    token = {
        liquidated_collateral_amount_in_usd: "collateral_asset",
        debt_to_cover_in_usd: "debt_asset"
    }


class CompoundForks:
    mapping = {
        "venus": "0x38_0xfd36e2c2a6789db23113685031d7f16329158384",
        "cream": "0x38_0x589de0f0ccf905477646599bb3e5c622c84cc0ba",
        "compound": "0x1_0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b"
    }
    bsc = ["venus", "cream"]
    eth = ["compound"]
    ftm = []
    polygon = []
    chain = {
        "0x38": bsc,
        "0x1": eth,
        "0xfa": ftm,
        "0x89": polygon
    }


class RemoveToken:
    tokens = [
        "0x1985365e9f78359a9b6ad760e32412f4a445e862",
        "0x70d8929d04b60af4fb9b58713ebcf18765ade422",
        "0xb91a659e88b51474767cd97ef3196a3e7cedd2c8",
        "0x78366446547d062f45b4c0f320cdaa6d710d87bb",
    ]


class GraphCreditScoreConfigKeys:
    wallet_statistics = 'wallet_statistics_v3'
    token_statistics = 'token_statistics'
    multichain_wallets_flagged_state = 'multichain_wallets_flagged_state'
    wallets_flagged_state = 'wallets_flagged_state'
    monitor_day = 'monitor_day'


class ArangoIndexConstant:
    multichain_wallet_flagged = 'multichain_wallet_flagged'
    multichain_wallet_scores_addresses = 'multichain_wallet_scores_addresses'
    multichain_wallet_scores_flagged = 'multichain_wallet_scores_flagged'


class ChainConstant:
    bsc_chain_id = '0x38'
    bsc_chain_name = 'BSC'
    polygon_chain_id = '0x89'
    polygon_chain_name = 'POLYGON'
    eth_chain_id = '0x1'
    eth_chain_name = 'ETHEREUM'
    ftm_chain_id = '0xfa'
    ftm_chain_name = 'FTM'
    all = [ftm_chain_id, polygon_chain_id, bsc_chain_id, eth_chain_id]
    names = [ftm_chain_name, polygon_chain_name, bsc_chain_name, eth_chain_name]


class TimeConstant:
    A_HOUR = 60 * 60
    A_DAY = A_HOUR * 24
    DAYS_7 = A_DAY * 7
    DAYS_30 = A_DAY * 30


class TokenCollections:
    mappings = {
        'market_caps': {'value': 'marketCap', 'logs': 'marketCapChangeLogs'},
        'trading_volumes': {'value': 'tradingVolume24h', 'logs': 'dailyTradingVolumes'},
        'price': {'value': 'price', 'logs': 'priceChangeLogs'},
        'price_stability': {'value': 'priceStability', 'logs': 'priceStabilityChangeLogs'},
        'credit_score': {'value': 'creditScore', 'logs': 'creditScoreChangeLogs'},
        'credit_score_x1': {'value': 'creditScorex1', 'logs': 'creditScorex1ChangeLogs'},
        'credit_score_x2': {'value': 'creditScorex2', 'logs': 'creditScorex2ChangeLogs'},
        'credit_score_x3': {'value': 'creditScorex3', 'logs': 'creditScorex3ChangeLogs'},
        'credit_score_x4': {'value': 'creditScorex4', 'logs': 'creditScorex4ChangeLogs'},
        'credit_score_x5': {'value': 'creditScorex5', 'logs': 'creditScorex5ChangeLogs'},
        'credit_score_x6': {'value': 'creditScorex6', 'logs': 'creditScorex6ChangeLogs'},
        'credit_score_x7': {'value': 'creditScorex7', 'logs': 'creditScorex7ChangeLogs'}
    }


class WalletStatisticFieldConstant:
    total_asset = 'total_asset'
    age_of_account = 'age_of_account'
    transaction_amount = 'transaction_amount'
    frequency_of_transaction = 'frequency_of_transaction'
    deposit = 'deposit'
    borrow = 'borrow'
    frequency_of_dapp_transaction='frequency_of_dapp_transaction'
    number_of_dapps='number_of_dapps'


class WalletCreditScoreWeightConstantV3:
    a1 = 0.25
    a2 = 0.25
    a3 = 0.1
    a4 = 0.15
    a5 = 0.1
    a6 = 0.1
    a7 = 0.05

    b11 = 0.04
    b12 = 0.96
    b21 = 0.25
    b22 = 0.25
    b23 = 0.25
    b24 = 0.25
    b31 = 0.5
    b32 = 0.5
    b41 = 0.4
    b42 = 0.2
    b43 = 0.4
    b51 = 1.0
    b52 = 1.0
    b61 = 1.0
    b71 = 0.6
    b72 = 0.4


class WalletCreditScoreWeightConstant:
    a1 = 0.3
    a2 = 0.4
    a3 = 0.15
    a4 = 0.1
    a5 = 0.05

    b11 = 0.04
    b12 = 0.96
    b21 = 0.4
    b22 = 0.1
    b23 = 0.2
    b24 = 0.1
    b25 = 0.2
    b31 = 0.6
    b32 = 0.4
    b41 = 1.0
    b51 = 0.6
    b52 = 0.4


class WalletCreditScoreFunctionConstant:
    score_low = 500
    score_high = 1000
    asset_high = 2e8
    amount_high = 15e7
    frequency_high = 3000
    timestamp_ethereum_beginning = 1438244788


class KDaysWeightConstant:
    a1 = 0.6
    a2 = 0.25
    a3 = 0.1
    a4 = 0.05
