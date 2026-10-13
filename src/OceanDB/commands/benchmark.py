"""Thin CLI for the maintained benchmark package."""

import json

import click

from OceanDB.benchmark.config import load


@click.group("benchmark")
def benchmark_group():
    """Prepare and run fixed core/exhaustive workloads; store results externally."""


@benchmark_group.command("check")
@click.argument("config", type=click.Path(exists=True, dir_okay=False))
def check_command(config):
    """Validate CONFIG without preparing databases or timing queries."""
    from OceanDB.benchmark.database import check

    result = check(load(config))
    result["source"] = {"sha256": result["source"]["sha256"]}
    click.echo(json.dumps(result, indent=2, default=str))


@benchmark_group.command("prepare")
@click.argument("config", type=click.Path(exists=True, dir_okay=False))
@click.option("--output", required=True, type=click.Path())
def prepare_command(config, output):
    """Create owned isolated clones and freeze a new run."""
    from OceanDB.benchmark.database import prepare

    result = prepare(load(config), output)
    click.echo(json.dumps(dict(status=result["status"], run_id=result["run_id"])))


@benchmark_group.command("run")
@click.argument("run", type=click.Path(exists=True, file_okay=False))
def run_command(run):
    """Execute a prepared schedule once; never resumes started measurements."""
    from pathlib import Path

    from OceanDB.benchmark.report import report
    from OceanDB.benchmark.runner import run as execute

    execute(run)
    click.echo(json.dumps(report(run, Path(run).with_name(Path(run).name + "-report"))))


@benchmark_group.command("report")
@click.argument("run", type=click.Path(exists=True, file_okay=False))
@click.option("--output", required=True, type=click.Path())
def report_command(run, output):
    """Rebuild a portable HTML/ZIP entirely from saved data."""
    from OceanDB.benchmark.report import report

    click.echo(json.dumps(report(run, output)))
