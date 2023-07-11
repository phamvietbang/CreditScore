from sqlalchemy import Column, Integer, String, MetaData, Table, Numeric

from config import PostgresqlDBConfig
from constants.event_constants import TransferEvent, WrappedToken


class SQLTables:
    metadata = MetaData(schema=PostgresqlDBConfig.SCHEMA)
    token_transfer = Table(
        "token_transfer",
        metadata,
        Column(TransferEvent.block_number, Integer, primary_key=True),
        Column(TransferEvent.log_index, Integer, primary_key=True),
        Column(TransferEvent.value, Numeric),
        Column(TransferEvent.transaction_hash, String),
        Column(TransferEvent.to_address, String),
        Column(TransferEvent.from_address, String),
        Column(TransferEvent.contract_address, String)
    )
    wrapped_token_event = Table(
        "wrapped_token",
        metadata,
        Column(WrappedToken.block_number, Integer, primary_key=True),
        Column(WrappedToken.log_index, Integer, primary_key=True),
        Column(WrappedToken.value, Numeric),
        Column(WrappedToken.transaction_hash, String),
        Column(WrappedToken.address, String),
        Column(WrappedToken.contract_address, String),
        Column(WrappedToken.event_type, String),
        Column(WrappedToken.wallet, String)
    )
    token_decimals = Table(
        "token_decimals",
        metadata,
        Column("address", String, primary_key=True),
        Column("decimals", Integer)
    )
    config = Table(
        "config",
        metadata,
        Column("id", String, primary_key=True),
        Column("start_extracting_at_block_number", Integer),
        Column("last_updated_at_block_number", Integer)
    )
    lido_transfer_share = Table(
        "lido_transfer_share",
        metadata,
        Column(TransferEvent.block_number, Integer, primary_key=True),
        Column(TransferEvent.log_index, Integer, primary_key=True),
        Column(TransferEvent.value, Numeric),
        Column(TransferEvent.transaction_hash, String),
        Column(TransferEvent.to_address, String),
        Column(TransferEvent.from_address, String),
        Column(TransferEvent.contract_address, String)
    )