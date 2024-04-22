import subprocess

from typing import List

from eldorado.logging_config import logger
from eldorado.my_dataclasses import Pod5Directory
from eldorado.utils import is_in_queue


def cleanup_merge_lock_files(pod5_dir: Pod5Directory):
    # Return if lock file does not exist
    if not pod5_dir.merge_lock_file.exists():
        return

    # Return if merge is in queue
    if pod5_dir.merge_job_id_file.exists():
        job_id = pod5_dir.merge_job_id_file.read_text().strip()
        if is_in_queue(job_id):
            return

    pod5_dir.merge_lock_file.unlink()


def submit_merging_to_slurm(pod5_dir: Pod5Directory, dry_run: bool):

    bam_batch_files = pod5_dir.bam_batches_dir.glob("*/*.bam")
    bam_batch_files_str = " ".join([str(x) for x in bam_batch_files])

    manifest_files = pod5_dir.bam_batches_dir.glob("*/pod5_manifest.txt")
    manifest_files_str = " ".join([str(x) for x in manifest_files])

    # Construct SLURM job script
    cores = 4
    slurm_script = f"""\
#!/bin/bash
#SBATCH --account           MomaDiagnosticsHg38
#SBATCH --time              12:00:00
#SBATCH --cpus-per-task     {cores}
#SBATCH --mem               32g
#SBATCH --mail-type         FAIL
#SBATCH --mail-user         simon.drue@clin.au.dk
#SBATCH --output            {pod5_dir.merge_script_file}.%j.out

        set -eu
        
        # Trap lock file
        trap 'rm -f {pod5_dir.merge_lock_file}' EXIT
        
        # Log start time
        START=$(date '+%Y-%m-%d %H:%M:%S')
        START_S=$(date '+%s')

        # Create output directory
        OUTDIR=$(dirname {pod5_dir.merged_bam})
        mkdir -p $OUTDIR

        # Create temp bam on scratch
        TEMP_BAM_FILE="$TEMPDIR/out.bam"

        # Run merge
        samtools merge \\
            --threads {cores} \\
            -f \\
            -o ${{TEMP_BAM_FILE}} \\
            {bam_batch_files_str}

        # Move temp file to output 
        mv ${{TEMP_BAM_FILE}} {pod5_dir.merged_bam}

        # Log end time
        END=$(date '+%Y-%m-%d %H:%M:%S')
        END_S=$(date +%s)
        RUNTIME=$((END_S-START_S))

        # Get size of output
        OUTPUT_BAM_SIZE=$(du -sL {pod5_dir.merged_bam} | cut -f1)
        BAM_READ_COUNT=$(samtools view -c {pod5_dir.merged_bam})

        # Write log file
        LOG_FILE={pod5_dir.merge_log_file}
        echo "output_bam={pod5_dir.merged_bam}" >> ${{LOG_FILE}}
        echo "output_bam_size=$OUTPUT_BAM_SIZE" >> ${{LOG_FILE}}
        echo "bam_read_count=$BAM_READ_COUNT" >> ${{LOG_FILE}}
        echo "slurm_job_id=$SLURM_JOB_ID" >> ${{LOG_FILE}}
        echo "start=$START" >> ${{LOG_FILE}}
        echo "end=$END" >> ${{LOG_FILE}}
        echo "runtime=$RUNTIME" >> ${{LOG_FILE}}

        # Create done file
        touch {pod5_dir.merge_done_file}

    """

    # Write Slurm script to a file
    pod5_dir.merge_script_file.parent.mkdir(exist_ok=True, parents=True)
    with open(pod5_dir.merge_script_file, "w", encoding="utf-8") as f:
        logger.info("Writing Slurm script to %s", str(pod5_dir.merge_script_file))
        f.write(slurm_script)

    if dry_run:
        logger.info("Dry run. Skipping submission of merging job.")
        return

    # Submit the job using Slurm
    job_id = subprocess.run(
        ["sbatch", "--parsable", str(pod5_dir.merge_script_file)],
        capture_output=True,
        check=True,
    )

    # Create .lock files
    pod5_dir.merge_lock_file.parent.mkdir(exist_ok=True, parents=True)
    pod5_dir.merge_lock_file.touch()

    with open(pod5_dir.merge_job_id_file, "w", encoding="utf-8") as f:
        f.write(job_id.stdout.decode().strip())


def get_pod5_dirs_for_merging(pod5_dirs: List[Pod5Directory]) -> List[Pod5Directory]:

    return [
        d
        for d in pod5_dirs
        if not d.merge_done_file.exists()
        and not d.merge_lock_file.exists()
        and all_existing_batches_are_done(d)
        and d.all_pod5_files_transfered()
        and all_existing_pod5_files_basecalled(d)
    ]


def all_existing_batches_are_done(pod5_dir: Pod5Directory) -> bool:
    batch_dirs = (d for d in pod5_dir.bam_batches_dir.glob("*") if d.is_dir())
    batch_done_files = (d / "batch.done" for d in batch_dirs)
    return all(f.exists() for f in batch_done_files)


def all_existing_pod5_files_basecalled(pod5_dir: Pod5Directory) -> bool:
    done_files = [x.name for x in pod5_dir.get_done_files()]
    return all(f"{x.name}.done" in done_files for x in pod5_dir.get_pod5_files())
