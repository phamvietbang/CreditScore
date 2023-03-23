import click
from cli.export_liquidated_wallet import export_liquidated_wallet


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


cli.add_command(export_liquidated_wallet, "export_liquidated_wallet")
