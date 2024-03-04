import csv
import time
from dataclasses import asdict
from pathlib import Path
from typing import List

import pandas as pd
import typer
from typing_extensions import Annotated

from eldorado.basecaller import submit_basecalling_to_slurm
from eldorado.demux import submit_demux_to_slurm
from eldorado.logging_config import get_log_file_handler, logger
from eldorado.model_selector import model_selector
from eldorado.my_dataclasses import SequencingRun

# Set up the CLI
app = typer.Typer()


@app.command()
def run_basecaller(
    sample_sheet: Annotated[
        Path,
        typer.Option(
            "--sample-sheet",
            "-s",
            help="Path to sample sheet",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
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
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Dry run",
        ),
    ] = False,
):
    # Setup logging to file
    if log_file is not None:
        file_handler = get_log_file_handler(log_file=log_file)
        logger.addHandler(file_handler)

    # TODO: This is slow
    sequencing_runs = get_runs_from_run_overview(sample_sheet)

    for run in sequencing_runs:

        # Check if any basecalling outputs already exist
        any_existing_bam_outputs = any(run.pod5_dir.parent.glob("bam*/*.bam"))
        any_existing_fastq_outputs = any(run.pod5_dir.parent.glob("fastq*/*.fastq*"))
        if any_existing_bam_outputs or any_existing_fastq_outputs:
            logger.debug(f"Skipping {run.pod5_dir}. Basecaller output already exists.")
            continue

        # Check if lock file exists
        if run.lock_file_basecall().exists():
            logger.debug(f"Skipping {run.pod5_dir}. Lock file: {run.lock_file_basecall} already exists.")
            continue

        # Check if data is done transfering
        pod5_files = run.pod5_dir.glob("*.pod5")
        newest_pod5_file = max(pod5_files, key=lambda x: x.stat().st_mtime)

        # Check if the newest file is less than 30 minutes old
        thirty_minutes = 30 * 60
        time_since_data_last_modified = time.time() - newest_pod5_file.stat().st_mtime
        if time_since_data_last_modified < thirty_minutes:
            logger.debug(
                f"Skipping {run.pod5_dir}. Data is not done transfering. Data was last modified {time_since_data_last_modified/60} minutes ago."
            )
            continue

        # Submit job
        models = model_selector(run)
        submit_basecalling_to_slurm(
            basecalling_model=models[0],
            modified_bases_models=models[1],
            pod5_dir=run.pod5_dir,
            output_bam=run.output_bam(),
            script_dir=Path("."),
            dry_run=dry_run,
            script_name=run.get_script_name_basecall(),
            lock_file=run.lock_file_basecall(),
        )


@app.command()
def run_demux(
    sample_sheet: Annotated[
        Path,
        typer.Option(
            "--sample-sheet",
            "-s",
            help="Path to sample sheet",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
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
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-d",
            help="Dry run",
        ),
    ] = False,
):
    # Setup logging to file
    if log_file is not None:
        file_handler = get_log_file_handler(log_file=log_file)
        logger.addHandler(file_handler)

    sequencing_runs = get_runs_from_run_overview(sample_sheet)

    for run in sequencing_runs:

        # TODO: Update logic

        # Check if any basecalling outputs already exist
        any_existing_bam_outputs = any(run.pod5_dir.parent.glob("bam*/*.bam"))
        any_existing_fastq_outputs = any(run.pod5_dir.parent.glob("fastq*/*.fastq*"))
        if any_existing_bam_outputs or any_existing_fastq_outputs:
            logger.debug(f"Skipping {run.pod5_dir}. Basecaller output already exists.")
            continue

        # Check if lock file exists
        if run.lock_file_basecall().exists():
            logger.debug(f"Skipping {run.pod5_dir}. Lock file: {run.lock_file_basecall} already exists.")
            continue

        # Check if data is done transfering
        pod5_files = run.pod5_dir.glob("*.pod5")
        newest_pod5_file = max(pod5_files, key=lambda x: x.stat().st_mtime)

        # Check if the newest file is less than 30 minutes old
        thirty_minutes = 30 * 60
        time_since_data_last_modified = time.time() - newest_pod5_file.stat().st_mtime
        if time_since_data_last_modified < thirty_minutes:
            logger.debug(
                f"Skipping {run.pod5_dir}. Data is not done transfering. Data was last modified {time_since_data_last_modified/60} minutes ago."
            )
            continue

        # Submit job
        submit_demux_to_slurm(
            raw_bam=run.output_bam(),
            kit=run.sequencing_kit,
            output_dir=run.output_dir_demux(),
            script_dir=Path("."),
            dry_run=dry_run,
            script_name=run.get_script_name_demux(),
            lock_file=run.lock_file_demux(),
            done_file=run.done_file_demux(),
        )


def get_runs_from_run_overview(sample_sheet: Path) -> List[SequencingRun]:

    # TODO: This is slow

    # Read pod5 directories from sample sheet
    samples = [Path(x) for x in pd.read_csv(sample_sheet)["pod5_dir"].tolist()]

    # Check if pod5 directories exist
    for sample in samples:
        if not sample.exists():
            raise ValueError(f"Pod5 directory for sample {sample} does not exist.")

    # Create sequencing runs
    sequencing_runs = [SequencingRun.create_from_pod5_dir(pod5_dir=sample) for sample in samples]
    return sequencing_runs


@app.command()
def update_finished_run_overview(
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
    run_overview: Annotated[
        Path,
        typer.Option(
            "--run-overview",
            "-o",
            help="Path to run overview",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ],
    log_file: Annotated[
        Path,
        typer.Option(
            "--log-file",
            "-l",
            help="Path to log file",
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
        ),
    ],
) -> None:

    # Setup logging to file
    if log_file is not None:
        file_handler = get_log_file_handler(log_file=log_file)
        logger.addHandler(file_handler)

    logger.info(f"Updating run overview: {run_overview}")
    logger.info(f"Using root dir: {root_dir}")

    # Get known pod5 dirs from run overview by reading column in csv file
    known_pod5_dirs = []
    if run_overview.exists():
        with open(run_overview, "r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            # Check for valid header in run overview
            if reader.fieldnames is None:
                raise ValueError(f"Fieldnames not found in run overview: {run_overview}")
            if "pod5_dir" not in reader.fieldnames:
                raise ValueError(f"'pod5_dir' not found in run overview: {run_overview}")

            # Collect known pod5 dirs
            for row in reader:
                known_pod5_dirs.append(Path(row["pod5_dir"]))

    # Get dirs with pod5 files
    all_pod5_dirs: List[Path] = []
    for pod5_dir in root_dir.glob(pattern="N[0-9][0-9][0-9]/*/*_*_*_*_*/pod5*"):
        if any(pod5_dir.glob("*.pod5")):
            all_pod5_dirs.append(pod5_dir)

    logger.info(f"Found {len(all_pod5_dirs)} pod5 dirs in total")

    # Get new runs
    new_pod5_dirs = set(all_pod5_dirs) - set(known_pod5_dirs)

    # Add new runs to run overview if any
    if new_pod5_dirs:

        logger.info(f"Adding {len(new_pod5_dirs)} new runs to run overview")

        new_runs = [SequencingRun.create_from_pod5_dir(x) for x in new_pod5_dirs]

        # Convert to dict
        new_runs_dict_list = [asdict(x) for x in new_runs]

        # Create header in run overview if it doesn't exist
        if not run_overview.exists():
            with open(run_overview, "w", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=new_runs_dict_list[0].keys())
                writer.writeheader()

        # Append new runs
        with open(run_overview, "a", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=new_runs_dict_list[0].keys())
            writer.writerows(new_runs_dict_list)
    else:
        logger.info("No new runs to add to run overview")

    logger.info("Done!")


if __name__ == "__main__":
    app()
