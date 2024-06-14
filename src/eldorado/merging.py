import subprocess
import textwrap

from typing import List

from pathlib import Path

from eldorado.logging_config import logger
from eldorado.pod5_handling import SequencingRun
from eldorado.utils import is_in_queue, write_to_file
from eldorado.filenames import BATCH_DONE, BATCH_BAM


def cleanup_merge_lock_files(pod5_dir: SequencingRun):
    # Return if lock file does not exist
    if not pod5_dir.merge_lock_file.exists():
        return

    # Return if merge is in queue
    if pod5_dir.merge_job_id_file.exists():
        job_id = pod5_dir.merge_job_id_file.read_text().strip()
        if is_in_queue(job_id):
            return

    pod5_dir.merge_lock_file.unlink()


def submit_merging_to_slurm(
    run: SequencingRun,
    mail_user: List[str],
    slurm_account: str,
    dry_run: bool,
) -> None:

    batch_dirs = get_done_batch_dirs(run)
    bam_files = [bam_file for batch_dir in batch_dirs for bam_file in batch_dir.glob(BATCH_BAM)]
    bam_files_str = " ".join([str(x) for x in bam_files])

    # Construct SLURM job script
    cores = 4
    slurm_script = f"""\
        #!/bin/bash
        #SBATCH --account           {slurm_account}
        #SBATCH --time              12:00:00
        #SBATCH --cpus-per-task     {cores}
        #SBATCH --mem               32g
        #SBATCH --mail-type         FAIL
        #SBATCH --mail-user         {mail_user[0]}
        #SBATCH --output            {run.merge_script_file}.%j.out
        #SBATCH --job-name          eldorado-merge-{run.metadata.library_pool_id}

        set -eu
        
        # Trap lock file
        trap 'rm -f {run.merge_lock_file}' EXIT

        # Create output directory
        OUTDIR="{run.merging_working_dir}"
        mkdir -p "$OUTDIR"

        # Create temp bam on scratch
        TEMP_BAM_FILE="$OUTDIR/tmp.bam.$SLURM_JOB_ID"

        # Run merge
        samtools merge \\
            --threads {cores} \\
            -o ${{TEMP_BAM_FILE}} \\
            {bam_files_str}

        # Move temp file to output 
        mv ${{TEMP_BAM_FILE}} {run.merged_bam}

        # Create done file
        touch {run.merge_done_file}

    """

    # Remove indent whitespace
    slurm_script = textwrap.dedent(slurm_script)

    # Write Slurm script to a file
    logger.info("Writing script to %s", str(run.merge_script_file))
    write_to_file(run.merge_script_file, slurm_script)

    if dry_run:
        logger.info("Dry run. Skipping submission of merging job.")
        return

    # Submit the job using Slurm
    std_out = subprocess.run(
        ["sbatch", "--parsable", str(run.merge_script_file)],
        capture_output=True,
        check=True,
    )

    # Create .lock files
    run.merge_lock_file.parent.mkdir(exist_ok=True, parents=True)
    run.merge_lock_file.touch()

    # Write job ID to file
    job_id = std_out.stdout.decode().strip()
    write_to_file(run.merge_job_id_file, job_id)

    logger.info("Submitted merging job to SLURM with job ID %s", job_id)


def merging_is_pending(run: SequencingRun) -> bool:

    return (
        not run.merge_done_file.exists()
        and not run.merge_lock_file.exists()
        and run.all_pod5_files_are_transferred()
        and all_pod5_files_are_basecalled(run)
    )


def all_pod5_files_are_basecalled(pod5_dir: SequencingRun) -> bool:
    done_files = [x.name for x in pod5_dir.get_done_files()]
    return all(f"{x.name}.done" in done_files for x in pod5_dir.get_transferred_pod5_files())


def get_done_batch_dirs(run: SequencingRun) -> list[Path]:
    # Batch dirs
    batch_dirs = [d for d in run.basecalling_batches_dir.glob("*") if d.is_dir()]

    # Filter out batch dirs that do not have a done file
    return [d for d in batch_dirs if (d / BATCH_DONE).exists()]
