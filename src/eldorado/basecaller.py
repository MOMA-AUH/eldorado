import subprocess
from pathlib import Path
from typing import List

from eldorado.constants import DORADO_EXECUTABLE


def submit_slurm_job(
    script_name: str,
    pod5_dir: Path,
    basecalling_model: Path,
    modified_bases_models: List[Path],
    output_bam: Path,
    script_dir: Path,
    dry_run: bool,
    lock_file: Path,
    done_file: Path,
):

    # Make dirs
    script_dir.mkdir(exist_ok=True)
    output_bam.parent.mkdir(exist_ok=True)

    # Construct SLURM job script
    modified_bases_models_arg = ""
    if modified_bases_models:
        modified_bases_models_arg = "--modified-bases-models" + ",".join([str(x) for x in modified_bases_models])

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

        {DORADO_EXECUTABLE} basecaller \
            --no-trim \
            {modified_bases_models_arg} \
            {basecalling_model} \
            {pod5_dir} \
        > {output_bam}

        # Remove lock file
        rm {lock_file}

        # Create done file
        touch {done_file}
    """

    # Write Slurm script to a file
    script_file = script_dir / script_name
    with open(script_file, "w", encoding="utf-8") as f:
        f.write(slurm_script)

    if dry_run:
        print(f"----------------------------- Dry run: {script_file.name} -----------------------------")
        print(slurm_script)
        return

    # Submit the job using Slurm
    subprocess.run(["sbatch", script_file], check=True)
