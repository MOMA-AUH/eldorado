import subprocess
import textwrap

from typing import List

from pathlib import Path

from eldorado.constants import BARCODING_KITS
from eldorado.logging_config import logger
from eldorado.pod5_handling import SequencingRun
from eldorado.utils import is_in_queue, write_to_file


def submit_demux_to_slurm(
    run: SequencingRun,
    sample_sheet: Path,
    dry_run: bool,
    slrum_account: str,
    mail_user: List[str],
):
    # Handle sample sheet without alias and barcode
    if sample_sheet_is_valid(sample_sheet):
        sample_sheet_option = f"--sample-sheet {sample_sheet}"
    else:
        sample_sheet_option = ""

    # Construct SLURM job script
    slurm_script = f"""\
        #!/bin/bash
        #SBATCH --account           {slrum_account}
        #SBATCH --time              12:00:00
        #SBATCH --cpus-per-task     16
        #SBATCH --mem               128g
        #SBATCH --mail-type         FAIL
        #SBATCH --mail-user         {mail_user[0]}
        #SBATCH --output            {run.demux_script_file}.%j.out
        #SBATCH --job-name          eldorado-demux-{run.metadata.library_pool_id}

        set -eu

        # Make sure .lock is removed when job is done
        trap 'rm {run.demux_lock_file}' EXIT

        # Create output directory
        OUTDIR="{run.demux_working_dir}"
        mkdir -p "$OUTDIR"

        # Create temp bam on scratch
        TMPDIR="$OUTDIR/tmp.$SLURM_JOB_ID"
        mkdir -p "$TMPDIR"
        
        # Run demux
        {run.config.dorado_executable} demux \\
            --no-trim \\
            {sample_sheet_option} \\
            --kit-name {run.metadata.sequencing_kit} \\
            --threads 0 \\
            --output-dir ${{TMPDIR}} \\
            {run.merged_bam}

        # Move output files from temp dir to output 
        mv ${{TMPDIR}}/*.bam {run.demux_working_dir}

        # Create done file
        touch {run.demux_done_file}

    """

    # Remove indent whitespace
    slurm_script = textwrap.dedent(slurm_script)

    # Write Slurm script to a file
    logger.info("Writing script to %s", str(run.demux_script_file))
    write_to_file(run.demux_script_file, slurm_script)

    if dry_run:
        logger.info("Dry run. Skipping submission of job to Slurm.")
        return

    # Submit the job using Slurm
    std_out = subprocess.run(
        ["sbatch", "--parsable", str(run.demux_script_file)],
        capture_output=True,
        check=True,
    )

    # Create .lock file
    run.demux_lock_file.parent.mkdir(parents=True, exist_ok=True)
    run.demux_lock_file.touch()

    # Write job ID to file
    job_id = std_out.stdout.decode().strip()
    write_to_file(run.demux_job_id_file, job_id)

    logger.info("Submitted job to Slurm with ID %s", job_id)


def demultiplexing_is_pending(run: SequencingRun) -> bool:
    return (
        not run.demux_done_file.exists()
        and not run.demux_lock_file.exists()
        and run.dorado_config_file.exists()
        and run.merge_done_file.exists()
        and run.merged_bam.exists()
    )


def process_demultiplexing(
    run: SequencingRun,
    mail_user: List[str],
    slurm_account: str,
    dry_run: bool,
):
    # Skip demultiplexing if sequencing kit is not a barcoding kit
    if run.metadata.sequencing_kit not in BARCODING_KITS:
        logger.info("Kit %s is not a barcoding kit.", run.metadata.sequencing_kit)
        logger.info("Skipping demultiplexing and using merged BAM file as final output.")

        # Move merged BAM file to output dir. Use library pool ID as filename
        run.demux_working_dir.mkdir(parents=True, exist_ok=True)
        run.merged_bam.rename(run.demux_working_dir / f"{run.metadata.library_pool_id}.bam")

        # Create done file
        run.demux_done_file.parent.mkdir(parents=True, exist_ok=True)
        run.demux_done_file.touch()
        return

    # Get sample sheet
    sample_sheet = run.get_sample_sheet()
    if sample_sheet is None:
        logger.error("Sample sheet not found for %s. Waiting for sample sheet to be uploaded.", run.metadata.library_pool_id)
        return

    # Submit job to Slurm
    submit_demux_to_slurm(
        run=run,
        sample_sheet=sample_sheet,
        dry_run=dry_run,
        mail_user=mail_user,
        slrum_account=slurm_account,
    )


def sample_sheet_is_valid(sample_sheet: Path) -> bool:

    # Read first line (header) of sample sheet csv
    with open(sample_sheet, "r", encoding="utf-8") as f:
        header = f.readline().split(",")
        header = [field.strip() for field in header]

    # Check header has required fields
    # See: https://community.nanoporetech.com/docs/prepare/library_prep_protocols/experiment-companion-minknow/v/mke_1013_v1_revdc_11apr2016/sample-sheet-upload
    required_fields = ["barcode", "alias"]
    if any(field not in header for field in required_fields):
        logger.warning("Sample sheet %s is missing required fields: %s", sample_sheet, required_fields)
        return False

    # Check if all entries in alias column are unique and < 40 characters (Error in Dorado if alias is > 40 characters)
    alias_column_index = header.index("alias")
    aliases = set()
    with open(sample_sheet, "r", encoding="utf-8") as f:
        for line in f:
            alias = line.split(",")[alias_column_index].strip()
            if alias in aliases:
                logger.warning("Duplicate alias (%s) found in sample sheet %s", alias, sample_sheet)
                return False
            if len(alias) > 40:
                logger.warning("Alias (%s) is longer than 40 characters in sample sheet %s", alias, sample_sheet)
                return False
            aliases.add(alias)

    return True


def cleanup_demultiplexing_lock_files(pod5_dir: SequencingRun):
    # Return if lock file does not exist
    if not pod5_dir.demux_lock_file.exists():
        return

    # Return if job is still in queue
    if pod5_dir.demux_job_id_file.exists():
        job_id = pod5_dir.demux_job_id_file.read_text().strip()
        if is_in_queue(job_id):
            return

    pod5_dir.demux_lock_file.unlink()
