import subprocess
from pathlib import Path
from typing import List

from eldorado.constants import DORADO_EXECUTABLE


def submit_basecalling_to_slurm(
    script_name: str,
    pod5_dir: Path,
    basecalling_model: Path,
    modified_bases_models: List[Path],
    output_bam: Path,
    script_dir: Path,
    dry_run: bool,
    lock_file: Path,
):

    # Construct SLURM job script
    modified_bases_models_arg = ""
    if modified_bases_models:
        modified_bases_models_str = ",".join([str(x) for x in modified_bases_models])
        modified_bases_models_arg = f"--modified-bases-models {modified_bases_models_str}"

    slurm_script = f"""\
#!/bin/bash
#SBATCH --account           MomaDiagnosticsHg38
#SBATCH --time              7-00:00:00
#SBATCH --cpus-per-task     18
#SBATCH --mem               190g
#SBATCH --partition         gpu
#SBATCH --gres              gpu:1
#SBATCH --mail-type         FAIL
#SBATCH --mail-user         simon.drue@clin.au.dk

        # Make sure .lock is removed when job is done
        trap 'rm {lock_file}' EXIT
        
        set -eu

        # Log start time
        START=$(date '+%Y-%m-%d %H:%M:%S')
        START_S=$(date '+%s')

        
        # Create temp file
        mkdir -p $(dirname {output_bam})
        TEMP_BAM_FILE=$(mktemp {output_bam}.tmp.XXXXXXXX)

        # Trap temp file
        trap 'rm ${{TEMP_BAM_FILE}}' EXIT

        # Run basecaller
        {DORADO_EXECUTABLE} basecaller \
            --no-trim \
            {modified_bases_models_arg} \
            {basecalling_model} \
            {pod5_dir} \
        > ${{TEMP_BAM_FILE}}

        # Move temp file to output 
        mv ${{TEMP_BAM_FILE}} {output_bam}

        # Log end time
        END=$(date '+%Y-%m-%d %H:%M:%S')
        END_S=$(date +%s)
        RUNTIME=$((END_S-START_S))

        # Get size of input and output
        POD5_SIZE=$(du -sL {pod5_dir} | cut -f1)
        POD5_COUNT=$(ls -1 {pod5_dir} | grep ".pod5" | wc -l)
        OUTPUT_BAM_SIZE=$(du -sL {output_bam} | cut -f1)

        # Write log file
        LOG_FILE={output_bam}.eldorado.basecaller.log
        echo "pod5_dir={pod5_dir}" > ${{LOG_FILE}}
        echo "pod5_size=$POD5_SIZE" >> ${{LOG_FILE}}
        echo "pod5_count=$POD5_COUNT" >> ${{LOG_FILE}}
        echo "output_bam={output_bam}" >> ${{LOG_FILE}}
        echo "output_bam_size=$OUTPUT_BAM_SIZE" >> ${{LOG_FILE}}
        echo "slurm_job_id=$SLURM_JOB_ID" >> ${{LOG_FILE}}
        echo "start=$START" >> ${{LOG_FILE}}
        echo "end=$END" >> ${{LOG_FILE}}
        echo "runtime=$RUNTIME" >> ${{LOG_FILE}}
        echo "basecaller={DORADO_EXECUTABLE}" >> ${{LOG_FILE}}
        echo "basecalling_model={basecalling_model}" >> ${{LOG_FILE}}
        echo "modified_bases_models={modified_bases_models}" >> ${{LOG_FILE}}

    """

    if dry_run:
        print(f"----------------------------- Dry run: {script_name} -----------------------------")
        print(slurm_script)
        return

    # Write Slurm script to a file
    script_dir.mkdir(exist_ok=True)
    script_file = script_dir / script_name
    with open(script_file, "w", encoding="utf-8") as f:
        f.write(slurm_script)

    # Create .lock file
    lock_file.touch()

    # Submit the job using Slurm
    subprocess.run(["sbatch", script_file], check=True)
