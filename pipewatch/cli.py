"""pipewatch CLI entry point.

Provides the main command-line interface for monitoring and alerting
on ETL pipeline health metrics.
"""

import click
from pipewatch import __version__


@click.group()
@click.version_option(version=__version__, prog_name="pipewatch")
def cli():
    """pipewatch — Monitor and alert on ETL pipeline health metrics."""
    pass


@cli.command()
@click.argument("pipeline", required=False)
@click.option("--interval", "-i", default=60, show_default=True,
              help="Polling interval in seconds.")
@click.option("--config", "-c", default="pipewatch.yaml", show_default=True,
              help="Path to configuration file.")
def watch(pipeline, interval, config):
    """Watch one or all pipelines for health metric violations."""
    target = pipeline or "all pipelines"
    click.echo(f"Watching {target} every {interval}s (config: {config})")
    # TODO: wire up metric collection loop


@cli.command()
@click.argument("pipeline", required=False)
@click.option("--config", "-c", default="pipewatch.yaml", show_default=True,
              help="Path to configuration file.")
@click.option("--format", "-f", "fmt",
              type=click.Choice(["table", "json", "csv"], case_sensitive=False),
              default="table", show_default=True,
              help="Output format.")
def status(pipeline, config, fmt):
    """Show the current health status of one or all pipelines."""
    target = pipeline or "all pipelines"
    click.echo(f"Status for {target} (format: {fmt}, config: {config})")
    # TODO: fetch and render latest metrics


@cli.command()
@click.option("--config", "-c", default="pipewatch.yaml", show_default=True,
              help="Path to configuration file.")
def check(config):
    """Validate the configuration file and exit."""
    click.echo(f"Validating config: {config}")
    # TODO: parse and validate config schema


if __name__ == "__main__":
    cli()
