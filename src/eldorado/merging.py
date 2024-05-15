import subprocess

from eldorado.logging_config import logger
from eldorado.pod5_handling import SequencingRun
from eldorado.utils import is_in_queue


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
    mail_user: str,
    dry_run: bool,
):

    bam_batch_files = run.basecalling_batches_dir.glob("*/*.bam")
    bam_batch_files_str = " ".join([str(x) for x in bam_batch_files])

    # Construct SLURM job script
    cores = 4
    slurm_script = f"""\
#!/bin/bash
#SBATCH --account           MomaDiagnosticsHg38
#SBATCH --time              12:00:00
#SBATCH --cpus-per-task     {cores}
#SBATCH --mem               32g
#SBATCH --mail-type         FAIL
#SBATCH --mail-user         {mail_user}
#SBATCH --output            {run.merge_script_file}.%j.out
#SBATCH --name              eldorado-merge

        set -eu
        
        # Trap lock file
        trap 'rm -f {run.merge_lock_file}' EXIT

        # Create output directory
        OUTDIR=$(dirname {run.merged_bam})
        mkdir -p $OUTDIR

        # Create temp bam on scratch
        TEMP_BAM_FILE="$TEMPDIR/out.bam"

        # Run merge
        samtools merge \\
            --threads {cores} \\
            -o ${{TEMP_BAM_FILE}} \\
            {bam_batch_files_str}

        # Move temp file to output 
        mv ${{TEMP_BAM_FILE}} {run.merged_bam}

        # Create done file
        touch {run.merge_done_file}

    """

    # Write Slurm script to a file
    run.merge_script_file.parent.mkdir(exist_ok=True, parents=True)
    with open(run.merge_script_file, "w", encoding="utf-8") as f:
        logger.info("Writing Slurm script to %s", str(run.merge_script_file))
        f.write(slurm_script)

    if dry_run:
        logger.info("Dry run. Skipping submission of merging job.")
        return

    # Submit the job using Slurm
    job_id = subprocess.run(
        ["sbatch", "--parsable", str(run.merge_script_file)],
        capture_output=True,
        check=True,
    )

    # Create .lock files
    run.merge_lock_file.parent.mkdir(exist_ok=True, parents=True)
    run.merge_lock_file.touch()

    with open(run.merge_job_id_file, "w", encoding="utf-8") as f:
        f.write(job_id.stdout.decode().strip())


def merging_is_pending(run: SequencingRun) -> bool:

    return (
        not run.merge_done_file.exists()
        and not run.merge_lock_file.exists()
        and all_existing_batches_are_done(run)
        and run.all_pod5_files_transferred()
        and all_existing_pod5_files_basecalled(run)
    )


def all_existing_batches_are_done(pod5_dir: SequencingRun) -> bool:
    batch_dirs = (d for d in pod5_dir.basecalling_batches_dir.glob("*") if d.is_dir())
    batch_done_files = (d / "batch.done" for d in batch_dirs)
    return all(f.exists() for f in batch_done_files)


def all_existing_pod5_files_basecalled(pod5_dir: SequencingRun) -> bool:
    done_files = [x.name for x in pod5_dir.get_done_files()]
    return all(f"{x.name}.done" in done_files for x in pod5_dir.get_transferred_pod5_files())
