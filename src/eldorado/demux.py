import subprocess
from pathlib import Path

from src.eldorado.constants import BARCODING_KITS, DORADO_EXECUTABLE
from src.eldorado.logging_config import logger

sequencing_kit = "sqk-nbd114-96"
sequencing_kit.upper()


is_kit_in_kits = next((x for x in BARCODING_KITS if x == sequencing_kit.upper()), "")


def submit_demux_to_slurm(
    script_name: str,
    raw_bam: Path,
    output_dir: Path,
    script_dir: Path,
    dry_run: bool,
    lock_file: Path,
    done_file: Path,
    kit: str,
):

    # Construct SLURM job script
    slurm_script = f"""\
#!/bin/bash
#SBATCH --account           MomaDiagnosticsHg38
#SBATCH --time              12:00:00
#SBATCH --cpus-per-task     16
#SBATCH --mem               128g
#SBATCH --mail-type         FAIL
#SBATCH --mail-user         simon.drue@clin.au.dk

        # Make sure .lock is removed when job is done
        trap 'rm {lock_file}' EXIT
        
        set -eu

        # Log start time
        START=$(date '+%Y-%m-%d %H:%M:%S')
        START_S=$(date '+%s')
        
        # Create temp file
        TEMP_DIR=$(mktemp -d {output_dir}.tmp.XXXXXXXX)

        # Trap temp file
        trap 'rm -rf ${{TEMP_DIR}}' EXIT

        # Run basecaller
        {DORADO_EXECUTABLE} demux \\
            --no-trim \\
            --verbose \\
            --kit {kit} \\
            --output-dir ${{TEMP_DIR}} \\
            {raw_bam}

        # Move output files from temp dir to output 
        mv ${{TEMP_DIR}}/*.bam {output_dir}

        # Create done file
        touch {done_file}

        # Log end time
        END=$(date '+%Y-%m-%d %H:%M:%S')
        END_S=$(date +%s)
        RUNTIME=$((END_S-START_S))

        # Write log file
        LOG_FILE={output_dir}.eldorado.demux.log
        echo "slurm_job_id=$SLURM_JOB_ID" >> ${{LOG_FILE}}
        echo "basecaller={DORADO_EXECUTABLE}" >> ${{LOG_FILE}}
        echo "output_bam={output_dir}" >> ${{LOG_FILE}}
        echo "start=$START" >> ${{LOG_FILE}}
        echo "end=$END" >> ${{LOG_FILE}}
        echo "runtime=$RUNTIME" >> ${{LOG_FILE}}

    """

    # Write Slurm script to a file
    script_dir.mkdir(exist_ok=True, parents=True)
    script_file = script_dir / script_name
    with open(script_file, "w", encoding="utf-8") as f:
        f.write(slurm_script)

    if dry_run:
        logger.info("Dry run. Skipping submission of job to Slurm.")
        return

    # Create .lock file
    lock_file.touch()

    # Submit the job using Slurm
    subprocess.run(["sbatch", script_file], check=True)
