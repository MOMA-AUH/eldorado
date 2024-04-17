from pathlib import Path

from typing import List

import typer
from typing_extensions import Annotated

from eldorado.basecalling import process_unbasecalled_pod5_dirs, get_pod5_dirs_for_basecalling, cleanup_stalled_batch_basecalling_dirs
from eldorado.merging import get_pod5_dirs_for_merging, submit_merging_to_slurm
from eldorado.logging_config import get_log_file_handler, logger
from eldorado.my_dataclasses import Pod5Directory
from eldorado.constants import MODIFICATION_OPTIONS

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
    ] = [],
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
            cleanup_stalled_batch_basecalling_dirs(pod5_dir)
            process_unbasecalled_pod5_dirs(pod5_dir, dry_run, modifications)

    # Merge bams for finished samples
    if pod5_dirs_for_merging := get_pod5_dirs_for_merging(pod5_dirs):
        logger.info("Found %d pod5 dirs for merging", len(pod5_dirs_for_merging))

        for pod5_dir in pod5_dirs_for_merging:
            logger.info("Processing %s", str(pod5_dir.path))

            # TODO: Add cleanup of stalled merging dirs
            # TODO: Add samtools merge command

            submit_merging_to_slurm(
                script_file=pod5_dir.script_dir / "merge_bams.sh",
                bam_dir=pod5_dir.bam_batches_dir,
                output_bam=pod5_dir.bam,
                dry_run=dry_run,
                lock_file=pod5_dir.merge_lock_file,
            )

    # Demultiplex samples
    # TODO: Implement this
    # TODO: Check if samples are merged
    # TODO: Check if run kit is a barcoding kit
    # TODO: Check for sample sheet


def find_pod5_dirs(root_dir: Path, pattern: str) -> List[Pod5Directory]:
    return [Pod5Directory(pod5_dir) for pod5_dir in list(root_dir.glob(pattern=pattern))]


if __name__ == "__main__":
    app()
