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
    eth = "eth"
    mapping = {
        bsc: "0x38",
        ftm: "0xfa",
        eth: "0x1"
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
    chain = {
        "0x38": bsc,
        "0x1": eth
    }
