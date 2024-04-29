from pathlib import Path

from typing import List

import typer
from typing_extensions import Annotated

import csv

from eldorado.basecalling import process_unbasecalled_pod5_files, needs_basecalling, cleanup_basecalling_lock_files
from eldorado.merging import needs_merging, submit_merging_to_slurm, cleanup_merge_lock_files
from eldorado.cleanup import needs_cleanup, cleanup_output_dir
from eldorado.logging_config import get_log_file_handler, logger
from eldorado.constants import MODIFICATION_OPTIONS, DORADO_EXECUTABLE
from eldorado.demultiplexing import needs_demultiplexing, process_demultiplexing, cleanup_demultiplexing_lock_files
from eldorado.pod5_handling import find_sequencning_runs_for_processing
from eldorado.configuration import get_dorado_config

# Set up the CLI
app = typer.Typer()


@app.command()
def scheduler(
    root_dir: Annotated[
        Path,
        typer.Option(
            "--root-dir",
            "-r",
            help="Root directory",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
        ),
    ],
    pattern: Annotated[
        str,
        typer.Option(
            "--pattern",
            "-p",
            help="Pattern used to search for pod5 dirs",
        ),
    ],
    modifications: Annotated[
        List[str] | None,
        typer.Option(
            "--mods",
            "-m",
            help=f"Comma separated list of modifications. Options: {', '.join(MODIFICATION_OPTIONS)}",
        ),
    ] = None,
    project_configs: Annotated[
        Path | None,
        typer.Option(
            "--project-config",
            "-c",
            help="Path to project config file (.csv)",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    min_batch_size: Annotated[
        int,
        typer.Option(
            "--min-batch-size",
            "-b",
            help="Minimum batch size",
        ),
    ] = 1,
    log_file: Annotated[
        Path | None,
        typer.Option(
            "--log-file",
            "-l",
            help="Path to log file",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
    run_basecalling: Annotated[
        bool,
        typer.Option(
            "--run-basecalling",
            help="Run basecalling",
        ),
    ] = False,
    run_merging: Annotated[
        bool,
        typer.Option(
            "--run-merging",
            help="Run merging",
        ),
    ] = False,
    run_demultiplexing: Annotated[
        bool,
        typer.Option(
            "--run-demultiplexing",
            help="Run demultiplexing",
        ),
    ] = False,
    run_cleanup: Annotated[
        bool,
        typer.Option(
            "--run-cleanup",
            help="Run cleanup",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Dry run",
        ),
    ] = False,
) -> None:

    # Check if everything should be run (default behaviour if all options are False)
    if not any([run_basecalling, run_merging, run_demultiplexing, run_cleanup]):
        run_basecalling = True
        run_merging = True
        run_demultiplexing = True
        run_cleanup = True

    # Handle modifications
    if modifications is None:
        modifications = []

    if modifications:
        for mod in modifications:
            if mod not in MODIFICATION_OPTIONS:
                typer.echo(f"Invalid modification: {mod}. Please choose from: {', '.join(MODIFICATION_OPTIONS)}")
                raise typer.Exit()
        # Filter to unique values
        modifications = list(set(modifications))

    # Setup logging to file
    if log_file is not None:
        file_handler = get_log_file_handler(log_file=log_file)
        logger.addHandler(file_handler)

    logger.info("Processing root dir: %s", str(root_dir))

    # Find pod5 dirs that needs processing
    runs = find_sequencning_runs_for_processing(root_dir, pattern)
    logger.info("Found %d pod5 dir(s) that needs processing", len(runs))

    # Load project configs from csv
    if project_configs is not None:
        logger.info("Loading project configs from %s", str(project_configs))
        # Read csv
        with open(project_configs, "r", encoding="utf-8") as f:
            project_configs_dict = csv.DictReader(f)
            project_configs_dict = {row["project_id"]: row for row in project_configs_dict}
    else:
        project_configs_dict = None

    # Process sequencing runs
    for run in runs:
        logger.info("Processing %s", str(run.pod5_dir))

        # Clean up lock files before processing
        cleanup_basecalling_lock_files(run)
        cleanup_merge_lock_files(run)
        cleanup_demultiplexing_lock_files(run)

        # Setup Dorado config
        if not run.dorado_config_file.exists():
            logger.info("Setting up Dorado config for %s", str(run.pod5_dir))
            metadata = run.metadata
            dorado_config = get_dorado_config(metadata, modifications, DORADO_EXECUTABLE)
            dorado_config.save(run.dorado_config_file)

        # Basecalling
        if run_basecalling and needs_basecalling(run):
            logger.info("Running basecalling...")
            process_unbasecalled_pod5_files(
                run=run,
                min_batch_size=min_batch_size,
                dry_run=dry_run,
            )

        # Merging
        if run_merging and needs_merging(run):
            logger.info("Running merging...")
            submit_merging_to_slurm(run, dry_run)

        # Demultiplexing
        if run_demultiplexing and needs_demultiplexing(run):
            logger.info("Running demultiplexing...")
            process_demultiplexing(
                run=run,
                dry_run=dry_run,
            )

        # Cleanup
        if run_cleanup and needs_cleanup(run):
            logger.info("Finalizing output...")
            cleanup_output_dir(run)


if __name__ == "__main__":
    app()
