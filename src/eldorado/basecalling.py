import re
import subprocess
from pathlib import Path
from typing import List

from eldorado.constants import DORADO_EXECUTABLE, MODELS_DIR
from eldorado.logging_config import logger
from eldorado.my_dataclasses import Pod5Directory, BasecallingBatch


def cleanup_stalled_batch_basecalling_dirs(pod5_dir: Pod5Directory):

    # Loop through all batch directories and collect inactive and active pod5 files
    active_pod5_files: set[Path] = set()
    stalled_batch_dirs: set[Path] = set()
    for batch_dir in pod5_dir.bam_batches_dir.glob("*"):

        # Skip if bam already exists
        if any(batch_dir.glob("*.bam")):
            continue

        # Skip if job is done
        if Path(batch_dir / "batch.done").exists():
            continue

        # Mark as stalled if slurm id file is missing
        slurm_id_file = batch_dir / "slurm_id.txt"
        if not slurm_id_file.exists():
            stalled_batch_dirs.add(batch_dir)
            continue

        # Mark as stalled if pod5 manifest is missing
        pod5_manifest_file = batch_dir / "pod5_manifest.txt"
        if not pod5_manifest_file.exists():
            stalled_batch_dirs.add(batch_dir)
            continue

        # Mark as stalled if job is not in queue
        job_id = get_slurm_job_id(slurm_id_file)
        if not is_in_queue(job_id):
            stalled_batch_dirs.add(batch_dir)
            continue

        # Collect pod5 files for active job
        pod5_files = read_pod5_manifest(pod5_manifest_file)
        active_pod5_files.update(pod5_files)

    # Remove lock files for pod5 files that are not in active batch directories
    for lock_file in pod5_dir.get_lock_files():
        if lock_file.name not in [f"{x.name}.lock" for x in active_pod5_files]:
            lock_file.unlink()

    # Remove stalled batch directories
    for batch_dir in stalled_batch_dirs:
        logger.info("Removing stalled batch directory %s", str(batch_dir))
        subprocess.run(["rm", "-rf", str(batch_dir)], check=True)


def read_pod5_manifest(pod5_manifest_file):
    with open(pod5_manifest_file, "r", encoding="utf-8") as f:
        pod5_files = [Path(x.strip()) for x in f]
    return pod5_files


def get_slurm_job_id(slurm_id_file: Path) -> str:
    with open(slurm_id_file, "r", encoding="utf-8") as f:
        job_id = f.read().strip()
    return job_id


def is_completed(job_id):
    res = subprocess.run(
        [
            "sacct",
            "--job",
            str(job_id),
            "--parsable2",
            "--format",
            "state",
            "--noheader",
        ],
        check=False,
        capture_output=True,
    )

    return res.returncode == 0 and "COMPLETED" in str(res.stdout.strip())


def is_in_queue(job_id):
    res = subprocess.run(
        ["squeue", "--job", str(job_id)],
        check=False,
        capture_output=True,
    )
    return res.returncode == 0


def process_unbasecalled_pod5_dirs(pod5_dir: Pod5Directory, dry_run: bool, modifications: List[str]):

    pod5_files_to_process = pod5_dir.get_pod5_files_for_basecalling()

    # Skip if no pod5 files to basecall
    if not pod5_files_to_process:
        logger.info("No pod5 files to basecall")
        return

    basecalling_batch = BasecallingBatch(
        pod5_files=pod5_files_to_process,
        batches_dir=pod5_dir.bam_batches_dir,
        pod5_lock_files_dir=pod5_dir.basecalling_lock_files_dir,
        pod5_done_files_dir=pod5_dir.basecalling_done_files_dir,
    )

    # Submit job
    basecalling_model = get_basecalling_model(pod5_dir)
    modified_bases_models = get_modified_bases_models(basecalling_model.name, MODELS_DIR, modifications)
    submit_basecalling_batch_to_slurm(
        batch=basecalling_batch,
        basecalling_model=basecalling_model,
        modified_bases_models=modified_bases_models,
        dry_run=dry_run,
    )


def get_pod5_dirs_for_basecalling(pod5_dirs: List[Pod5Directory]) -> List[Pod5Directory]:

    # Keep only pod5 directories that has pod5 files
    pod5_dirs = [x for x in pod5_dirs if contains_pod5_files(x.path)]

    # Keep only pod5 dirs that are not done basecalling
    pod5_dirs = [x for x in pod5_dirs if not is_basecalling_complete(x.path)]

    return pod5_dirs


def contains_pod5_files(x: Path) -> bool:
    return any(x.glob("*.pod5"))


def is_basecalling_complete(pod5_dir: Path) -> bool:
    any_existing_bam_outputs = any(pod5_dir.parent.glob("bam*/*.bam"))
    any_existing_fastq_outputs = any(pod5_dir.parent.glob("fastq*/*.fastq*"))

    return any_existing_bam_outputs or any_existing_fastq_outputs


def is_version_newer(current_version, candidate_version):
    """
    Compare two version strings.

    Returns:
        False if current_version => candidate_version
        True if current_version < candidate_version
    """
    current_components = [int(x) for x in current_version.split(".")]
    candidate_components = [int(x) for x in candidate_version.split(".")]

    # Pad the shorter version with zeros
    while len(current_components) < len(candidate_components):
        current_components.append(0)
    while len(candidate_components) < len(current_components):
        candidate_components.append(0)

    # Compare each component
    for current, candidate in zip(current_components, candidate_components):
        if current > candidate:
            return False
        elif current < candidate:
            return True

    # If all components are equal, return False
    return False


def extract_version(path: Path) -> str:
    pattern = r"@v([\d+\.]*\d+)$"
    return re.findall(pattern, path.name)[0]


def get_latest_version(models: List[Path]) -> Path:
    """
    Find the latest version of a list of paths.
    """
    # Initialize
    latest_path = models[0]
    latest_version = extract_version(latest_path)

    # If there are more than one model in the list compare the versions
    for path in models[1:]:
        version = extract_version(path)
        if version and (latest_version is None or is_version_newer(latest_version, version)):
            latest_version = version
            latest_path = path

    return latest_path


def get_basecalling_model(run: Pod5Directory) -> Path:

    # Find basecalling model
    # Link to model documentation: https://github.com/nanoporetech/dorado?tab=readme-ov-file#dna-models
    basecalling_model = None

    run_metadata = run.get_run_metadata()

    # If run on FLO-PRO114M (R10.4.1) flow cell
    if run_metadata.flow_cell_product_code == "FLO-PRO114M":
        if run_metadata.sample_rate == 4000:
            basecalling_model = MODELS_DIR / "dna_r10.4.1_e8.2_400bps_hac@v4.1.0"
        elif run_metadata.sample_rate == 5000:
            basecalling_model = MODELS_DIR / "dna_r10.4.1_e8.2_400bps_hac@v4.3.0"

    # If run on FLO-PRO002 (R9.4.1) flow cell
    elif run_metadata.flow_cell_product_code == "FLO-PRO002":
        basecalling_model = MODELS_DIR / "dna_r9.4.1_e8_hac@v3.3"

    # Check if basecalling model was found
    if basecalling_model is None:
        raise ValueError(f"Could not find basecalling model for flow cell {run_metadata.flow_cell_product_code}")
    # Check if basecalling model exists
    if not basecalling_model.exists():
        raise ValueError(f"Basecalling model {basecalling_model} does not exist")

    return basecalling_model


def get_modified_bases_models(basecalling_model: str, model_dir: Path, modifications: List[str]) -> List[Path]:

    modified_bases_models = []

    # Get all modified base models based on base model
    for mod in modifications:
        # If more than one model found, select the latest one and add it to the list
        if mod_models := list(model_dir.glob(f"{basecalling_model}*{mod}*")):
            latest_mod_model = get_latest_version(mod_models)
            modified_bases_models.append(latest_mod_model)

    return modified_bases_models


def submit_basecalling_batch_to_slurm(
    batch: BasecallingBatch,
    basecalling_model: Path,
    modified_bases_models: List[Path],
    dry_run: bool,
):
    # Convert path lists to strings
    pod5_files_str = " ".join([str(x) for x in batch.pod5_files])
    lock_files_str = " ".join([str(x) for x in batch.pod5_lock_files])
    done_files_str = " ".join([str(x) for x in batch.pod5_done_files])

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
#SBATCH --output            {batch.script_file}.%j.out

        # Set traps
        trap 'rm {lock_files_str}' EXIT
        
        set -eu

        # Log start time
        START=$(date '+%Y-%m-%d %H:%M:%S')
        START_S=$(date '+%s')

        # Create output directory
        OUTDIR=$(dirname {batch.bam})
        mkdir -p $OUTDIR

        # Create temp output file
        TEMP_BAM_FILE=$(mktemp {batch.bam}.tmp.XXXXXXXX)

        # Create dir on scratch
        POD5_DIR_TEMP=$TEMP/pod5
        mkdir -p $POD5_DIR_TEMP
        
        # Copy pod5 files to scratch
        POD5_FILES_LIST=({pod5_files_str})
        for POD5_FILE in ${{POD5_FILES_LIST[@]}}
        do
            ln -s $POD5_FILE $POD5_DIR_TEMP
        done

        # Run basecaller
        {DORADO_EXECUTABLE} basecaller \\
            --no-trim \\
            {modified_bases_models_arg} \\
            {basecalling_model} \\
            $POD5_DIR_TEMP \\
        > ${{TEMP_BAM_FILE}}

        # Move temp file to output 
        mv ${{TEMP_BAM_FILE}} {batch.bam}

        # Log end time
        END=$(date '+%Y-%m-%d %H:%M:%S')
        END_S=$(date +%s)
        RUNTIME=$((END_S-START_S))

        # Get size of input and output
        POD5_SIZE=$(du -sL {pod5_files_str} | cut -f1)
        POD5_COUNT=$(ls -1 {pod5_files_str} | grep ".pod5" | wc -l)
        OUTPUT_BAM_SIZE=$(du -sL {batch.bam} | cut -f1)
        BAM_READ_COUNT=$(samtools view -c {batch.bam})

        # Write log file
        LOG_FILE={batch.bam}.eldorado.basecaller.log
        echo "pod5_size=$POD5_SIZE" >> ${{LOG_FILE}}
        echo "pod5_count=$POD5_COUNT" >> ${{LOG_FILE}}
        echo "output_bam={batch.bam}" >> ${{LOG_FILE}}
        echo "output_bam_size=$OUTPUT_BAM_SIZE" >> ${{LOG_FILE}}
        echo "bam_read_count=$BAM_READ_COUNT" >> ${{LOG_FILE}}
        echo "slurm_job_id=$SLURM_JOB_ID" >> ${{LOG_FILE}}
        echo "start=$START" >> ${{LOG_FILE}}
        echo "end=$END" >> ${{LOG_FILE}}
        echo "runtime=$RUNTIME" >> ${{LOG_FILE}}
        echo "basecaller={DORADO_EXECUTABLE}" >> ${{LOG_FILE}}
        echo "basecalling_model={basecalling_model}" >> ${{LOG_FILE}}
        echo "modified_bases_models={modified_bases_models}" >> ${{LOG_FILE}}

        # Touch done files
        BATCH_DONE_FILE={batch.done_file}
        mkdir -p $(dirname $BATCH_DONE_FILE)
        touch $BATCH_DONE_FILE
        
        DONE_FILES_LIST=({done_files_str})
        for DONE_FILE in ${{DONE_FILES_LIST[@]}}
        do
            mkdir -p $(dirname $DONE_FILE)
            touch $DONE_FILE
        done    
        

    """

    # Write Slurm script to a file
    batch.script_file.parent.mkdir(exist_ok=True, parents=True)
    with open(batch.script_file, "w", encoding="utf-8") as f:
        logger.info("Writing Slurm script to %s", str(batch.script_file))
        f.write(slurm_script)

    # Write pod5 manifest
    with open(batch.pod5_manifest, "w", encoding="utf-8") as f:
        for pod5_file in batch.pod5_files:
            f.write(f"{pod5_file}\n")

    if dry_run:
        logger.info("Dry run. Skipping submission of basecalling job.")
        return

    # Create .lock files
    for lock_file in batch.pod5_lock_files:
        lock_file.parent.mkdir(exist_ok=True, parents=True)
        lock_file.touch()

    # Submit the job using Slurm
    job_id = subprocess.run(["sbatch", "--parsable", str(batch.script_file)], capture_output=True, check=True)

    # Write job id to file
    with open(batch.slurm_id_txt, "w", encoding="utf-8") as f:
        f.write(job_id.stdout.decode().strip())
