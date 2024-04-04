import csv
import hashlib
import re
import time
from dataclasses import asdict
from pathlib import Path
from typing import List

import pandas as pd
import typer
from typing_extensions import Annotated

from eldorado.basecaller import get_basecalling_model, get_modified_bases_models, submit_basecalling_to_slurm
from eldorado.demux import submit_demux_to_slurm
from eldorado.logging_config import get_log_file_handler, logger
from eldorado.my_dataclasses import SequencingRun

# Set up the CLI
app = typer.Typer()


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

        # TODO: Update logic so live basecalling becomes possible

        logger.info("Processing %s", run.pod5_dir)

        # Check if any basecalling outputs already exist
        any_existing_bam_outputs = any(run.pod5_dir.parent.glob("bam*/*.bam"))
        any_existing_fastq_outputs = any(run.pod5_dir.parent.glob("fastq*/*.fastq*"))
        if any_existing_bam_outputs or any_existing_fastq_outputs:
            logger.info("Skipping. Basecaller output already exists.")
            continue

        # # Check if lock file exists
        # if run.lock_file_basecall.exists():
        #     logger.info(
        #         "Skipping. Lock file: %s already exists.",
        #         run.lock_file_basecall,
        #     )
        #     continue

        # Check if data is done transfering
        pod5_files = run.pod5_dir.glob("*.pod5")
        newest_pod5_file = max(pod5_files, key=lambda x: x.stat().st_mtime)

        # Check if the newest file is less than 30 minutes old
        thirty_minutes = 30 * 60
        time_since_data_last_modified = time.time() - newest_pod5_file.stat().st_mtime
        if time_since_data_last_modified < thirty_minutes:
            logger.info(
                "Skipping. Data is not done transfering. Data was last modified %s minutes ago.",
                time_since_data_last_modified / 60,
            )
            continue

        # Submit job
        # submit_demux_to_slurm(
        #     raw_bam=run.output_bam,
        #     kit=run.sequencing_kit,
        #     output_dir=run.output_dir_demux,
        #     script_dir=Path("."),
        #     dry_run=dry_run,
        #     script_name=run.script_name_demux,
        #     lock_file=run.lock_file_demux,
        #     done_file=run.done_file_demux,
        # )


@app.command()
def find_all_finished_sequencing_runs(
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

    logger.info("Updating run overview: %s", run_overview)
    logger.info("Using root dir: %s", root_dir)

    # Get known pod5 dirs from run overview by reading column in csv file
    known_pod5_dirs: List[Path] = []
    if run_overview.exists():
        with open(run_overview, "r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            # Check for valid header in run overview
            if reader.fieldnames is None:
                raise ValueError(f"Fieldnames not found in run overview: {run_overview}")
            if "pod5_dir" not in reader.fieldnames:
                raise ValueError(f"'pod5_dir' not found in run overview: {run_overview}")

            # Collect known pod5 dirs
            known_pod5_dirs.extend(Path(row["pod5_dir"]) for row in reader)

    # Get pod5 dirs files
    all_pod5_dirs: List[Path] = list(root_dir.glob(pattern="N[0-9][0-9][0-9]/*/*_*_*_*_*/pod5"))

    # Filter pod5 dirs
    unprocessed_pod5_dirs = [x for x in all_pod5_dirs if x not in known_pod5_dirs and contains_pod5_files(x) and pod5_dir_is_finished_transfering(x)]

    logger.info("Found %s new pod5 dirs that are finished", len(unprocessed_pod5_dirs))

    # Add new runs to run overview if any
    if unprocessed_pod5_dirs:

        logger.info("Adding new runs to run overview")

        new_runs = [SequencingRun.create_from_pod5_dir(x) for x in unprocessed_pod5_dirs]

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


@app.command()
def find_all_unbasecalled_pod5_files(
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

    # Get pod5 dirs
    all_pod5_dirs: List[Path] = list(root_dir.glob(pattern="N[0-9][0-9][0-9]/*/*_*_*_*_*/pod5"))

    # Filter pod5 dirs
    unbasecalled_pod5_dirs = [x for x in all_pod5_dirs if contains_pod5_files(x) and not is_basecalling_complete(x)]

    logger.info("Found %s pod5 dirs that are not done basecalling", len(unbasecalled_pod5_dirs))

    # Add new runs to run overview if any
    for pod5_dir in unbasecalled_pod5_dirs:

        logger.info("Processing %s", pod5_dir)

        sequencing_run = SequencingRun.create_from_pod5_dir(pod5_dir)

        pod5_files_locked = sequencing_run.get_pod5_lock_files()
        pod5_files_done = sequencing_run.get_pod5_done_files()

        all_pod5_files = sequencing_run.get_pod5_files()

        # remove pod5 files for which there is a lock file
        aa = [x for x in all_pod5_files if sequencing_run.pod5_files_locked_dir.glob(f"{x.name}.lock")]

        # TODO: Add .lock and .done when looking in lock and done dirs
        unbasecalled_pod5_files = [
            x for x in sequencing_run.get_pod5_files() if x not in pod5_files_locked + pod5_files_done and is_done_transfering(x)
        ]

        # Skip if no pod5 files to basecall
        if not unbasecalled_pod5_files:
            logger.info("No pod5 files to basecall in %s", pod5_dir)
            continue

        # Create a md5 hash for bam part
        string = "".join([str(x) for x in unbasecalled_pod5_files])
        md5_hash = hashlib.md5(string.encode()).hexdigest()

        output_bam = sequencing_run.output_bam_parts_dir / f"{md5_hash}.bam"
        script_file = sequencing_run.script_dir / f"{md5_hash}.sh"

        lock_files = sequencing_run.lock_files_from_list(unbasecalled_pod5_files)

        # Submit job
        basecalling_model = get_basecalling_model(sequencing_run)
        modified_bases_models = get_modified_bases_models(basecalling_model)
        submit_basecalling_to_slurm(
            basecalling_model=basecalling_model,
            modified_bases_models=modified_bases_models,
            pod5_files=unbasecalled_pod5_files,
            output_bam=output_bam,
            dry_run=dry_run,
            script_file=script_file,
            lock_files=lock_files,
        )


def get_runs_from_run_overview(sample_sheet: Path) -> List[SequencingRun]:

    # Read pod5 directories from sample sheet
    pod5_dirs = [Path(x) for x in pd.read_csv(sample_sheet)["pod5_dir"].tolist()]

    # Check if pod5 directories exist
    for pod5_dir in pod5_dirs:
        if not pod5_dir.exists():
            raise ValueError(f"Pod5 directory {pod5_dir} does not exist.")

    return [SequencingRun.create_from_pod5_dir(pod5_dir=pod5_dir) for pod5_dir in pod5_dirs]


def pod5_dir_is_finished_transfering(pod5_dir: Path) -> bool:
    # Get final summary
    final_summary = next(pod5_dir.parent.glob("final_summary*.txt"), None)

    # If final summary does not exist raise error
    if final_summary is None:
        logger.info("Final summary not found in %s", pod5_dir.parent)
        return False

    # Read final summary
    with open(final_summary, "r", encoding="utf-8") as file:
        file_content = file.read()

    # Get number of pod5 files
    matches = re.search(r"pod5_files_in_final_dest=(\d+)", file_content)

    # If number of pod5 files is not found raise error
    if matches is None:
        logger.error("Number of pod5 files not found in %s", final_summary)
        return False

    # Get expected number of pod5 files
    n_pod5_files_expected = int(matches[1])

    # Count the number of pod5 files
    pod5_files = list(pod5_dir.glob("*.pod5"))
    n_pod5_files_count = len(pod5_files)

    # If number of pod5 files is not correct folder is not finished transfering
    if n_pod5_files_expected != n_pod5_files_count:
        logger.info(
            "Skipping %s. Data not done transfering. Expected %s pod5 files but found %s.",
            pod5_dir,
            n_pod5_files_expected,
            n_pod5_files_count,
        )
        return False

    # Check if the newest file is less than 30 minutes old
    newest_pod5_file = max(pod5_files, key=lambda x: x.stat().st_mtime)

    thirty_minutes = 30 * 60
    time_since_data_last_modified = time.time() - newest_pod5_file.stat().st_mtime
    if time_since_data_last_modified < thirty_minutes:
        logger.info(
            "Skipping %s. Data is not done transfering. Data was last modified %s minutes ago.",
            pod5_dir,
            time_since_data_last_modified / 60,
        )
        return False

    return True


def contains_pod5_files(x: Path) -> bool:
    return any(x.glob("*.pod5"))


def is_basecalling_complete(pod5_dir: Path) -> bool:
    any_existing_bam_outputs = any(pod5_dir.parent.glob("bam*/*.bam"))
    any_existing_fastq_outputs = any(pod5_dir.parent.glob("fastq*/*.fastq*"))

    return any_existing_bam_outputs or any_existing_fastq_outputs


def is_done_transfering(file: Path) -> bool:
    thirty_minutes = 30 * 60
    time_since_data_last_modified = time.time() - file.stat().st_mtime
    return time_since_data_last_modified > thirty_minutes


if __name__ == "__main__":
    app()
