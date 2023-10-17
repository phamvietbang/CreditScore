import logging

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from config import PostgresqlDBConfig
from constants.event_constants import TransferEvent
from constants.postgres_constants import SQLTables

_logger = logging.getLogger(__name__)

sql_table = PostgresqlDBConfig.POSTGRESQLDB_TABLE


class TransferPostgresqlStreamingExporter:
    def __init__(self, connection_url: str = None):
        # Set up the database connection and create the table
        if not connection_url:
            connection_url = PostgresqlDBConfig.CONNECTION_URL
        self.engine = create_engine(connection_url)

        # Create a session to manage the database transactions
        self.session = sessionmaker(bind=self.engine)()

    def get_decimals(self, keys):
        if not keys:
            return
        # conn = self.engine.connect()
        table = SQLTables.token_decimals
        data = self.session.execute(table.select().where(table.c.address.in_(keys)))
        return data.cursor

    def get_token_transfer(self, address, token, from_block, to_block):
        table = SQLTables.token_transfer
        query = f"""
        SELECT * FROM {PostgresqlDBConfig.SCHEMA}.token_transfer
        WHERE (block_number BETWEEN {from_block} AND {to_block})
        AND (from_address == {address} OR to_address == {address})
        AND contract_address == {token}
        """

        data = self.session.execute(query)
        return data.cursor

    def export_items(self, operations_data: list) -> None:
        if not operations_data:
            return
        table = SQLTables.token_transfer
        try:
            self.session.execute(insert(table, operations_data). \
                on_conflict_do_nothing(
                index_elements=[TransferEvent.log_index, TransferEvent.block_number],
            ))
        except Exception as e:
            print(e)
            raise e
        self.session.commit()

    def export_token_decimals(self, operations_data):
        if not operations_data:
            return
        # conn = self.engine.connect()
        table = SQLTables.token_decimals
        self.session.execute(insert(table, operations_data).on_conflict_do_nothing(index_elements=["address"]))
        self.session.commit()