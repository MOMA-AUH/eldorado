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

    bam_batch_files = pod5_dir.bam_batches_dir.glob("*.bam")
    bam_batch_files_str = " ".join([str(x) for x in bam_batch_files])

    # Construct SLURM job script
    slurm_script = f"""\
#!/bin/bash
#SBATCH --account           MomaDiagnosticsHg38
#SBATCH --time              12:00:00
#SBATCH --cpus-per-task     4
#SBATCH --mem               32g
#SBATCH --mail-type         FAIL
#SBATCH --mail-user         simon.drue@clin.au.dk
#SBATCH --output            {pod5_dir.merge_script_file}.%j.out


        set -eu
        
        # Make sure .lock files are removed when job is done
        trap 'rm {pod5_dir.merge_lock_file}' EXIT
        
        # Log start time
        START=$(date '+%Y-%m-%d %H:%M:%S')
        START_S=$(date '+%s')

        # Create output directory
        OUTDIR=$(dirname {pod5_dir.bam})
        mkdir -p $OUTDIR

        # Create temp output file
        TEMP_BAM_FILE=$(mktemp {pod5_dir.bam}.tmp.XXXXXXXX)
        
        # Trap temp files
        trap 'rm ${{TEMP_BAM_FILE}}' EXIT

        # Run merge
        samtools merge \\
            --threads 4 \\
            -o ${{TEMP_BAM_FILE}} \\
            {bam_batch_files_str}

        # Move temp file to output 
        mv ${{TEMP_BAM_FILE}} {pod5_dir.bam}

        # Log end time
        END=$(date '+%Y-%m-%d %H:%M:%S')
        END_S=$(date +%s)
        RUNTIME=$((END_S-START_S))

        # Get size of output
        OUTPUT_BAM_SIZE=$(du -sL {pod5_dir.bam} | cut -f1)
        BAM_READ_COUNT=$(samtools view -c {pod5_dir.bam})

        # Write log file
        LOG_FILE={pod5_dir.bam}.eldorado.basecaller.log
        echo "output_bam={pod5_dir.bam}" >> ${{LOG_FILE}}
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

    # Create .lock files
    pod5_dir.merge_lock_file.parent.mkdir(exist_ok=True, parents=True)
    pod5_dir.merge_lock_file.touch()

    # Submit the job using Slurm
    job_id = subprocess.run(["sbatch", "--parsable", str(pod5_dir.merge_script_file)], check=True)

    with open(pod5_dir.merge_job_id_file, "w", encoding="utf-8") as f:
        f.write(job_id.stdout.decode().strip())


def get_pod5_dirs_for_merging(pod5_dirs: List[Pod5Directory]) -> List[Pod5Directory]:

    pod5_dirs = [x for x in pod5_dirs if not x.merge_done_file.exists()]

    pod5_dirs = [x for x in pod5_dirs if not x.merge_lock_file.exists()]

    pod5_dirs = [x for x in pod5_dirs if are_all_files_basecalled(x)]

    return pod5_dirs


def are_all_files_basecalled(pod5_dir: Pod5Directory) -> bool:
    # Check if all pod5 files have been transferred
    if not pod5_dir.are_pod5_all_files_transfered():
        return False

    # Check if number of pod5 files is equal to number of done files
    # Count the number of pod5 files
    pod5_files = pod5_dir.get_pod5_files()
    n_pod5_files_expected = len(pod5_files)

    # Count the number of pod5 done files
    done_files = pod5_dir.get_done_files()
    n_pod5_files_count = len(done_files)

    # If number of pod5 files is euqal to expected number of pod5 files basecalling is done
    return n_pod5_files_expected == n_pod5_files_count
