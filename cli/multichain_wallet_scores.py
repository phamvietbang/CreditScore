import time
import click
from calculate.wallet_credit_score import WalletsCreditScore
from config import ArangoDBConfig, get_logger

logger = get_logger('Multichain Wallet Scores')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-g', '--graph', default=None, type=str,
              help='graph_db connection url example: bolt://localhost:7687@username:password')
@click.option('-f', '--statistic-path', default='data', show_default=True, type=str,
              help='Wallet statistic data path')
@click.option('-k', '--k', default=30, show_default=True, type=int, help='Number of day to calculate data')
@click.option('-h', '--h', default=10, show_default=True, type=int, help='Number of day to calculate ROI')
@click.option('-nc', '--n-cpu', default=1, show_default=True, type=int, help='Number of CPU')
@click.option('-c', '--cpu', default=1, show_default=True, type=int, help='CPU order')
@click.option('-bs', '--wallet-batch-size', default=10000, show_default=True, type=int, help='Wallet batch size')
@click.option('-w', '--max-workers', default=4, show_default=True, type=int,
              help='Max workers to get and update wallets')
@click.option('-l', '--loop', default=True, show_default=True, type=bool, help='Loop this action')
@click.option('-sl', '--sleep', default=86400, show_default=True, type=int, help='Sleep time')
def multichain_wallet_scores(graph, statistic_path, k, h, n_cpu, cpu, wallet_batch_size, max_workers,
                             loop, sleep):
    """Calculate all wallets credit score, update to knowledge graph."""

    if graph is None:
        graph = f'ArangoDB@{ArangoDBConfig.USERNAME}:{ArangoDBConfig.PASSWORD}@http://{ArangoDBConfig.ARANGODB_HOST}:{ArangoDBConfig.ARANGODB_PORT}'
    logger.info(f'Connected to graph: {graph}')

    while True:
        start_time = time.time()
        try:
            logger.info(f'Graph connected: {graph.split("@")[-1]}')
            calculator = WalletsCreditScore(
                graph=graph,
                statistic_path=statistic_path,
                k=k
            )

            calculator.calculate_multichain_wallets(
                h=h,
                max_workers=max_workers,
                n_cpu=n_cpu,
                cpu=cpu,
                wallet_batch_size=wallet_batch_size
            )
            del calculator
            if not loop:
                break
        except Exception as ex:
            logger.exception(ex)

        time_calculate_credit_score = time.time() - start_time

        # Send time calculate credit score all wallet to server monitoring

        logger.info("Send time calculate credit score all wallet to server monitoring......")

        sleep_time = max(sleep - time_calculate_credit_score, 0)
        logger.info(f'Sleep {sleep} seconds')
        time.sleep(sleep_time)

    logger.info('Done')
