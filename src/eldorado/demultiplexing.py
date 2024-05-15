import subprocess

from pathlib import Path

from eldorado.constants import BARCODING_KITS, DORADO_EXECUTABLE
from eldorado.logging_config import logger
from eldorado.pod5_handling import SequencingRun
from eldorado.utils import is_in_queue


def submit_demux_to_slurm(
    pod5_dir: SequencingRun,
    sample_sheet: Path,
    dry_run: bool,
    mail_user: str,
):

    # Construct SLURM job script
    slurm_script = f"""\
#!/bin/bash
#SBATCH --account           MomaDiagnosticsHg38
#SBATCH --time              12:00:00
#SBATCH --cpus-per-task     16
#SBATCH --mem               128g
#SBATCH --mail-type         FAIL,END
#SBATCH --mail-user         {mail_user}
#SBATCH --output            {pod5_dir.demux_script_file}.%j.out
#SBATCH --name              eldorado-demux

        # Make sure .lock is removed when job is done
        trap 'rm {pod5_dir.demux_lock_file}' EXIT
        
        set -eu

        # Run demux
        {DORADO_EXECUTABLE} demux \\
            --verbose \\
            --no-trim \\
            --sample-sheet {sample_sheet} \\
            --kit-name {pod5_dir.metadata.sequencing_kit} \\
            --threads 0 \\
            --output-dir ${{TEMPDIR}} \\
            {pod5_dir.merged_bam}

        # Move output files from temp dir to output 
        mv ${{TEMPDIR}}/*.bam {pod5_dir.output_dir}

        # Create done file
        touch {pod5_dir.demux_done_file}

    """

    # Write Slurm script to a file
    pod5_dir.demux_script_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pod5_dir.demux_script_file, "w", encoding="utf-8") as f:
        f.write(slurm_script)

    if dry_run:
        logger.info("Dry run. Skipping submission of job to Slurm.")
        return

    # Submit the job using Slurm
    job_id = subprocess.run(
        ["sbatch", str(pod5_dir.demux_script_file)],
        capture_output=True,
        check=True,
    )

    # Create .lock file
    pod5_dir.demux_lock_file.parent.mkdir(parents=True, exist_ok=True)
    pod5_dir.demux_lock_file.touch()

    # Write job ID to file
    with open(pod5_dir.demux_job_id_file, "w", encoding="utf-8") as f:
        f.write(job_id.stdout.decode().strip())


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
    mail_user: str,
    dry_run: bool,
):
    sample_sheet = run.get_sample_sheet_path()

    # Check if sample sheet exists
    if sample_sheet is None:
        logger.error("Sample sheet not found for %s. Skipping demultiplexing.", run.pod5_dir)
        return

    # Check if demultiplexing should be skipped
    skip_demultiplexing = False
    if not sample_sheet_has_alias_and_barcode(sample_sheet):
        skip_demultiplexing = True
        logger.error("Sample sheet %s does not have 'alias' and 'barcode' columns.", sample_sheet)

    if run.metadata.sequencing_kit not in BARCODING_KITS:
        skip_demultiplexing = True
        logger.info("Kit %s is not a barcoding kit.", run.metadata.sequencing_kit)

    # Check if demultiplexing should be skipped
    if skip_demultiplexing:
        logger.info("Skipping demultiplexing and using merged BAM file as final output.")

        # Move merged BAM file to output dir. Use library pool ID as filename
        run.output_dir.mkdir(parents=True, exist_ok=True)
        run.merged_bam.rename(run.output_dir / f"{run.metadata.library_pool_id}.bam")

        # Create done file
        run.demux_done_file.parent.mkdir(parents=True, exist_ok=True)
        run.demux_done_file.touch()
        return

    # Submit job to Slurm
    submit_demux_to_slurm(
        pod5_dir=run,
        sample_sheet=sample_sheet,
        dry_run=dry_run,
        mail_user=mail_user,
    )


def sample_sheet_has_alias_and_barcode(sample_sheet: Path) -> bool:

    # Read first line (header) of sample sheet
    with open(sample_sheet, "r", encoding="utf-8") as f:
        header = f.readline()

    # Check header has required fields
    # See: https://community.nanoporetech.com/docs/prepare/library_prep_protocols/experiment-companion-minknow/v/mke_1013_v1_revdc_11apr2016/sample-sheet-upload
    required_fields = ["barcode", "alias"]
    return all(field in header for field in required_fields)


def cleanup_demultiplexing_lock_files(pod5_dir: SequencingRun):
    # Return if lock file does not exist
    if not pod5_dir.demux_lock_file.exists():
        return

    # Return if job is still in queue
    if pod5_dir.demux_job_id_file.exists():
        job_id = pod5_dir.merge_job_id_file.read_text().strip()
        if is_in_queue(job_id):
            return

    pod5_dir.demux_lock_file.unlink()