import click

from database.mongodb import MongoDB
from job.export_liquidated_wallet_job import ExportLiquidatedWalletJob
from constants import Chain
from database.arangodb import ArangoDB


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-c', '--chain', default='bsc', show_default=True, type=str, help='Network name example bsc or polygon')
@click.option('-b', '--batch-size', default=100, show_default=True, type=int, help='Batch size')
@click.option('-i', '--importer-uri', type=str, help='mongo event uri')
@click.option('-e', '--exporter-uri', type=str, help='mongo storage uri')
@click.option('-t', '--token-database-uri', type=str, help='token database uri')
@click.option('-d', '--database', type=str, help='database name')
@click.option('-p', '--prefix', default='', type=str, help='database name')
def export_liquidated_wallet(chain, batch_size, importer_uri, exporter_uri, token_database_uri, database, prefix):
    """Enrich new wallets to graph."""

    _importer = MongoDB(importer_uri, database, prefix)
    _token_db = MongoDB(token_database_uri, "TokenDatabase")
    _exporter = MongoDB(exporter_uri, database, prefix)
    _arangodb = ArangoDB()

    job = ExportLiquidatedWalletJob(
        importer=_importer,
        exporter=_exporter,
        token_db=_token_db,
        batch_size=batch_size,
        chain_id=Chain.mapping[chain],
        arangodb=_arangodb
    )
    job.run()
