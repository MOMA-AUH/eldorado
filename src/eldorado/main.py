from pathlib import Path

from typing import List

import typer
from typing_extensions import Annotated

from eldorado.basecalling import process_unbasecalled_pod5_dirs, get_pod5_dirs_for_basecalling, cleanup_stalled_batch_basecalling_dirs_and_lock_files
from eldorado.merging import get_pod5_dirs_for_merging, submit_merging_to_slurm, cleanup_merge_lock_files
from eldorado.logging_config import get_log_file_handler, logger
from eldorado.constants import MODIFICATION_OPTIONS
from eldorado.utils import find_pod5_dirs

# Set up the CLI
app = typer.Typer()


@app.command()
def run_basecalling(
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
        List[str],
        typer.Option(
            "--mods",
            "-m",
            help=f"Comma separated list of modifications. Options: {', '.join(MODIFICATION_OPTIONS)}",
        ),
    ] = None,
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
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Dry run",
        ),
    ] = False,
) -> None:

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

    pod5_dirs = find_pod5_dirs(root_dir, pattern)

    # Basecall samples
    if pod5_dirs_for_basecalling := get_pod5_dirs_for_basecalling(pod5_dirs):
        logger.info("Found %d pod5 dirs for basecalling", len(pod5_dirs_for_basecalling))

        for pod5_dir in pod5_dirs_for_basecalling:
            logger.info("Processing %s", str(pod5_dir.path))
            cleanup_stalled_batch_basecalling_dirs_and_lock_files(pod5_dir)
            process_unbasecalled_pod5_dirs(pod5_dir, dry_run, modifications)

    # Merge bams for finished samples
    if pod5_dirs_for_merging := get_pod5_dirs_for_merging(pod5_dirs):
        logger.info("Found %d pod5 dirs for merging", len(pod5_dirs_for_merging))

        for pod5_dir in pod5_dirs_for_merging:
            logger.info("Processing %s", str(pod5_dir.path))
            cleanup_merge_lock_files(pod5_dir)
            submit_merging_to_slurm(pod5_dir, dry_run)

    # Demultiplex samples
    if False and pod5_dirs:
        logger.info("Found %d pod5 dirs for demultiplexing", len(pod5_dirs))

        for pod5_dir in pod5_dirs:
            logger.info("Processing %s", str(pod5_dir.path))

            print(pod5_dir.get_run_metadata())

    # TODO: Implement this
    # TODO: Check if samples are merged
    # TODO: Check if run kit is a barcoding kit
    # TODO: Check for sample sheet


if __name__ == "__main__":
    app()
