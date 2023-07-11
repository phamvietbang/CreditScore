import click

from database.klg_mongodb import MongoDbKLG
from database.mongodb import MongoDB
from job.export_liquidated_wallet_job import ExportLiquidatedWalletJob
from constants.constants import Chain


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='Batch size')
@click.option('-i', '--importer-uri', type=str, help='mongo event uri')
@click.option('-e', '--exporter-uri', type=str, help='mongo storage uri')
@click.option('-t', '--token-database-uri', type=str, help='token database uri')
@click.option('-k', '--klg-database-uri', type=str, help='klg database uri')
@click.option('-n', '--provider-node', type=str, help='provider node')
@click.option('-d', '--database', type=str, help='database name')
@click.option('-p', '--prefix', default='', type=str, help='database name')
def export_liquidated_wallet(chain, batch_size, importer_uri, exporter_uri, token_database_uri, provider_node,
                             klg_database_uri, database, prefix):
    """Enrich new wallets to graph."""
    _chain_id = Chain.mapping[chain]
    _importer = MongoDB(importer_uri, database, prefix)
    _token_db = MongoDB(token_database_uri, "TokenDatabase")
    _exporter = MongoDB(exporter_uri, database, prefix)
    _arangodb = MongoDbKLG(klg_database_uri, chain_id=_chain_id, chain_name=chain)

    job = ExportLiquidatedWalletJob(
        importer=_importer,
        exporter=_exporter,
        token_db=_token_db,
        batch_size=batch_size,
        chain_id=_chain_id,
        arangodb=_arangodb,
        provider_uri=provider_node
    )
    job.run()
