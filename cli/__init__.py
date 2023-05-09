import click
from cli.export_liquidated_wallet import export_liquidated_wallet
from cli.multichain_wallet_scores import multichain_wallet_scores


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(export_liquidated_wallet, "export_liquidated_wallet")
cli.add_command(multichain_wallet_scores, "multichain_wallet_scores")
