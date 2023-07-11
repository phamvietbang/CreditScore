class Event:
    type = 'type'
    event_type = 'event_type'
    contract_address = 'contract_address'
    token_addresses = "token_addresses"
    transaction_hash = 'transaction_hash'
    log_index = 'log_index'
    block_number = 'block_number'
    block_timestamp = 'block_timestamp'


class TransferEvent:
    type = 'type'
    event_type = 'event_type'
    contract_address = "contract_address"
    transaction_hash = 'transaction_hash'
    log_index = 'log_index'
    block_number = 'block_number'
    value = "value"
    from_address = "from_address"
    to_address = "to_address"
    asset = "asset"

class WrappedToken:
    type = 'type'
    event_type = 'event_type'
    contract_address = "contract_address"
    transaction_hash = 'transaction_hash'
    log_index = 'log_index'
    block_number = 'block_number'
    value = "value"
    address = "address"
    asset = "asset"
    wallet = "wallet"

class WRAPBNBTokenEvent:
    type = 'type'
    event_type = 'event_type'
    contract_address = "contract_address"
    transaction_hash = 'transaction_hash'
    log_index = 'log_index'
    block_number = 'block_number'
    value = "value"
    address = "address"
    asset = "asset"

class EventTypes:
    swap = 'SWAP'
    mint = 'MINT'
    burn = 'BURN'
    deposit = 'DEPOSIT'
    borrow = 'BORROW'
    withdraw = 'WITHDRAW'
    repay = 'REPAY'
    liquidate = 'LIQUIDATE'


class StreamerTypes:
    events = "events"
    lending_events = "lending_events"
    trava_lp_events = "trava_lp_events"
    lottery_transfer_events = 'lottery_transfer_events'
    lottery_end_events = 'lottery_end_events'
    all = [events, lending_events, trava_lp_events]


class LendingTypes:
    ethereum = "ethereum"
    bsc = "bsc"
    ftm = "ftm"
