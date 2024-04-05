from pathlib import Path
import re
import subprocess

from typing import List

from src.eldorado.logging_config import logger
from src.eldorado.my_dataclasses import SequencingRun


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


def get_pod5_dirs_for_merging(pod5_dirs: List[Path]) -> List[Path]:

    pod5_dirs = [x for x in pod5_dirs if is_done_basecalling(x)]

    return pod5_dirs


def is_done_basecalling(pod5_dir: Path) -> bool:
    # Get final summary
    final_summary = next(pod5_dir.parent.glob("final_summary*.txt"), None)

    # If final summary does not exist basecalling is not done
    if final_summary is None:
        return False

    # Read final summary
    with open(final_summary, "r", encoding="utf-8") as file:
        file_content = file.read()

    # Get number of pod5 files
    matches = re.search(r"pod5_files_in_final_dest=(\d+)", file_content)

    # If number of pod5 files is not found raise error
    if matches is None:
        logger.error("Could not find number of pod5 files in %s", final_summary)
        return False

    # Get expected number of pod5 files
    n_pod5_files_expected = int(matches[1])

    # Count the number of pod5 done files
    run = SequencingRun.create_from_pod5_dir(pod5_dir)

    done_files = run.get_pod5_done_files()
    n_pod5_files_count = len(done_files)

    # If number of pod5 files is euqal to expected number of pod5 files basecalling is done
    return n_pod5_files_expected == n_pod5_files_count
