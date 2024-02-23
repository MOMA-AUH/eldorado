import csv
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List

import typer
from typing_extensions import Annotated

from eldorado import basecaller
from eldorado.basecaller import submit_slurm_job
from eldorado.logging_config import get_log_file_handler, logger
from eldorado.model_selector import model_selector

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
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-n",
            help="Dry run",
        ),
    ] = False,
):

    sequencing_runs = get_runs_from_run_overview(sample_sheet)

    for run in sequencing_runs:

        # Check if run is already running or done
        if run.lock_file.exists() or run.done_file.exists():
            continue

        # Create .lock file
        run.lock_file.touch()

        # Submit job
        submit_slurm_job(
            basecalling_model=Path(run.base_model),
            modified_bases_models=[Path(x) for x in run.modified_bases_models],
            pod5_dir=run.pod5_dir,
            output_bam=run.output_bam,
            script_dir=Path("."),
            dry_run=dry_run,
            script_name=run.script_name,
            lock_file=run.lock_file,
            done_file=run.done_file,
        )


@dataclass
class SequencingRun:
    # Metadata
    run_id: str
    project_id: str
    sample_id: str

    # Data
    pod5_dir: Path

    # File metadata
    n_pod5_files: int = field(init=False)
    size_on_disk: int = field(init=False)

    # Slurm
    script_name: str = field(init=False)

    # Output bam
    output_bam: Path = field(init=False)

    # Flow files
    lock_file: Path = field(init=False)
    done_file: Path = field(init=False)

    # Models
    base_model: str = field(init=False)
    modified_bases_models: str = field(init=False)

    def __post_init__(self):
        # Get models
        base_model, modified_bases_models = model_selector()
        self.base_model = str(base_model)
        self.modified_bases_models = ",".join([str(x) for x in modified_bases_models])

        # Get gile metadata
        self.n_pod5_files = len(list(self.pod5_dir.glob("*.pod5")))
        self.size_on_disk = self.pod5_dir.stat().st_size

        # Get script name
        self.script_name = "dorado_basecalling" + self.project_id + "_" + self.sample_id + "_" + self.run_id + "_script.sh"

        # Place output dir/bam next to input pod5 directory
        self.output_bam = self.pod5_dir.parent / (self.pod5_dir.name.replace("pod5", "bam") + "_manual") / ("basecalled.bam")

        # Lock and done files
        self.lock_file = self.pod5_dir.parent / (self.pod5_dir.name + ".lock")
        self.done_file = self.pod5_dir.parent / (self.pod5_dir.name + ".done")


def get_runs_from_run_overview(sample_sheet: Path) -> List[SequencingRun]:
    samples = []

    # Loop over each row in the sample sheet
    with open(sample_sheet, "r", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            samples.append(
                SequencingRun(
                    project_id=row["project_id"],
                    sample_id=row["sample_id"],
                    run_id=row["run_id"],
                    pod5_dir=Path(row["pod5_dir"]),
                )
            )
    return samples


@app.command()
def update_run_overview(
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

    # pod5_dirs: List[Path] = []

    # for pod5_dir in root_dir.glob(pattern="N[0-9][0-9][0-9]/*/*_*_*_*_*/pod5*"):
    #     pod5_file_list = list(pod5_dir.glob("*.pod5"))
    #     if pod5_file_list:
    #         pod5_dirs.append(pod5_dir)

    cmd = f"find {root_dir} -mindepth 4 -maxdepth 4 -type d -name 'pod5*' -not -empty"

    logger.info(f"Running: {cmd}")

    find_res = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        check=False,
    )

    find_pod5_dirs = find_res.stdout.strip().split("\n")

    if find_res.returncode != 0:
        logger.error(f"Error: {find_res.stderr}")

    # Remove empty pod5 dirs
    pod5_dirs = [Path(x) for x in find_pod5_dirs if Path(x).glob("*.pod5")]

    logger.info(f"Found {len(pod5_dirs)} pod5 dirs")

    # Get run overview
    run_overview_list = []
    if run_overview.exists():
        run_overview_list = get_runs_from_run_overview(run_overview)

    known_pod5_dirs = {x.pod5_dir for x in run_overview_list}

    # Get new runs
    new_pod5_dirs = [x for x in pod5_dirs if x not in known_pod5_dirs]

    logger.info(f"Found {len(new_pod5_dirs)} new pod5 dirs")

    # Add new runs to run overview if any
    if new_pod5_dirs:

        new_runs = [
            SequencingRun(
                pod5_dir=x,
                project_id=x.parent.parent.parent.name,
                sample_id=x.parent.parent.name,
                run_id=x.parent.name,
            )
            for x in new_pod5_dirs
        ]

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


if __name__ == "__main__":
    app()
