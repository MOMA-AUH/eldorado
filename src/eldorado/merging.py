from pathlib import Path
import subprocess

from typing import List

from eldorado.logging_config import logger
from eldorado.my_dataclasses import Pod5Directory, is_file_inactive


def submit_merging_to_slurm(
    script_file: Path,
    bam_dir: Path,
    output_bam: Path,
    dry_run: bool,
    lock_file: Path,
):

    bam_batch_files = bam_dir.glob("*.bam")
    bam_batch_files_str = " ".join([str(x) for x in bam_batch_files])

    # TODO: Control logging to file

    # Construct SLURM job script
    slurm_script = f"""\
#!/bin/bash
#SBATCH --account           MomaDiagnosticsHg38
#SBATCH --time              12:00:00
#SBATCH --cpus-per-task     4
#SBATCH --mem               32g
#SBATCH --mail-type         FAIL
#SBATCH --mail-user         simon.drue@clin.au.dk

        # Make sure .lock files are removed when job is done
        trap 'rm {lock_file}' EXIT
        
        set -eu

        # Log start time
        START=$(date '+%Y-%m-%d %H:%M:%S')
        START_S=$(date '+%s')

        # Create output directory
        OUTDIR=$(dirname {output_bam})
        mkdir -p $OUTDIR

        # Create temp output file
        TEMP_BAM_FILE=$(mktemp {output_bam}.tmp.XXXXXXXX)
        
        # Trap temp files
        trap 'rm ${{TEMP_BAM_FILE}}' EXIT

        # Run merge
        samtools merge \\
            --threads 4 \\
            -o ${{TEMP_BAM_FILE}} \\
            {bam_batch_files_str}

        # Move temp file to output 
        mv ${{TEMP_BAM_FILE}} {output_bam}

        # Log end time
        END=$(date '+%Y-%m-%d %H:%M:%S')
        END_S=$(date +%s)
        RUNTIME=$((END_S-START_S))

        # Get size of input and output
        OUTPUT_BAM_SIZE=$(du -sL {output_bam} | cut -f1)

        # Write log file
        LOG_FILE={output_bam}.eldorado.basecaller.log
        echo "output_bam={output_bam}" >> ${{LOG_FILE}}
        echo "output_bam_size=$OUTPUT_BAM_SIZE" >> ${{LOG_FILE}}
        echo "slurm_job_id=$SLURM_JOB_ID" >> ${{LOG_FILE}}
        echo "start=$START" >> ${{LOG_FILE}}
        echo "end=$END" >> ${{LOG_FILE}}
        echo "runtime=$RUNTIME" >> ${{LOG_FILE}}

    """

    # Write Slurm script to a file
    script_file.parent.mkdir(exist_ok=True, parents=True)
    with open(script_file, "w", encoding="utf-8") as f:
        logger.info("Writing Slurm script to %s", script_file)
        f.write(slurm_script)

    if dry_run:
        logger.info("Dry run. Skipping submission of basecalling job.")
        return

    # Create .lock files
    lock_file.parent.mkdir(exist_ok=True, parents=True)
    lock_file.touch()

    # Submit the job using Slurm
    subprocess.run(["sbatch", script_file], check=True)


def get_pod5_dirs_for_merging(pod5_dirs: List[Pod5Directory]) -> List[Pod5Directory]:

    pod5_dirs = [x for x in pod5_dirs if are_all_files_basecalled(x)]

    pod5_dirs = [x for x in pod5_dirs if not has_merge_lock_file(x)]

    pod5_dirs = [x for x in pod5_dirs if are_all_bam_parts_done(x)]

    return pod5_dirs


def has_merge_lock_file(pod5_dir: Pod5Directory) -> bool:
    return pod5_dir.merge_lock_file.exists()


def are_all_files_basecalled(pod5_dir: Pod5Directory) -> bool:
    # Check if all pod5 files have been transferred
    if not pod5_dir.are_pod5_all_files_transfered():
        return False

    # Check if number of pod5 files is equal to number of done files
    # Count the number of pod5 files
    pod5_files = pod5_dir.get_pod5_files()
    n_pod5_files_expected = len(pod5_files)

    # Count the number of pod5 done files
    done_files = pod5_dir.get_pod5_done_files()
    n_pod5_files_count = len(done_files)

    # If number of pod5 files is euqal to expected number of pod5 files basecalling is done
    return n_pod5_files_expected == n_pod5_files_count


def are_all_bam_parts_done(pod5_dir: Pod5Directory) -> bool:
    bam_parts = pod5_dir.output_bam_parts_dir.glob("*.bam")
    five_min_in_sec = 5 * 60
    return not any(is_file_inactive(bam_part, five_min_in_sec) for bam_part in bam_parts)
