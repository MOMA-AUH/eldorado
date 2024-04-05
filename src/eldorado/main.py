from pathlib import Path

from typing import List

import typer
from typing_extensions import Annotated

from src.eldorado.basecalling import process_unbasecalled_pod5_dirs, get_pod5_dirs_for_basecalling
from src.eldorado.merging import get_pod5_dirs_for_merging, submit_merging_to_slurm
from src.eldorado.logging_config import get_log_file_handler, logger
from src.eldorado.my_dataclasses import SequencingRun

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

    # Setup logging to file
    if log_file is not None:
        file_handler = get_log_file_handler(log_file=log_file)
        logger.addHandler(file_handler)

    logger.info("Processing root dir: %s", root_dir)

    pod5_dirs = find_pod5_dirs(root_dir, pattern)

    # Basecall samples
    if pod5_dirs_for_basecalling := get_pod5_dirs_for_basecalling(pod5_dirs):
        logger.info("Found %s pod5 dirs for basecalling", len(pod5_dirs_for_basecalling))

        for pod5_dir in pod5_dirs_for_basecalling:
            logger.info("Processing %s", pod5_dir)
            process_unbasecalled_pod5_dirs(pod5_dir, dry_run)

    # Merge bams for finished samples
    if pod5_dirs_for_merging := get_pod5_dirs_for_merging(pod5_dirs):
        logger.info("Found %s pod5 dirs for merging", len(pod5_dirs_for_merging))
        for pod5_dir in pod5_dirs_for_merging:
            logger.info("Processing %s", pod5_dir)
            run = SequencingRun.create_from_pod5_dir(pod5_dir)

            submit_merging_to_slurm(
                script_file=run.script_dir / "merge_bams.sh",
                bam_dir=run.output_bam_parts_dir,
                output_bam=run.output_bam,
                dry_run=dry_run,
                lock_file=run.merge_lock_file,
            )

    # Demultiplex samples
    # TODO: Implement this


def find_pod5_dirs(root_dir: Path, pattern: str) -> List[Path]:
    return list(root_dir.glob(pattern=pattern))


if __name__ == "__main__":
    app()
