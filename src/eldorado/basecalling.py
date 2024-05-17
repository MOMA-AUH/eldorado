import subprocess
from pathlib import Path
from typing import List

import time

from dataclasses import dataclass, field

from eldorado.logging_config import logger
from eldorado.pod5_handling import SequencingRun
from eldorado.utils import is_in_queue, write_to_file
from eldorado.filenames import BATCH_DONE, BATCH_JOB_ID, BATCH_MANIFEST, BATCH_BAM, BATCH_LOG, BATCH_SCRIPT


@dataclass
class BasecallingBatch:
    run: SequencingRun

    # Derived attributes
    batch_id: str = field(init=False)

    pod5_files: List[Path] = field(init=False)

    working_dir: Path = field(init=False)

    output_bam: Path = field(init=False)
    log_file: Path = field(init=False)
    pod5_manifest: Path = field(init=False)
    slurm_id_file: Path = field(init=False)
    script_file: Path = field(init=False)
    done_file: Path = field(init=False)

    pod5_lock_files: List[Path] = field(init=False)
    pod5_done_files: List[Path] = field(init=False)

    def __post_init__(self):
        # Create batch id from current time
        self.batch_id = str(int(time.time()))

        # Get unbasecalled pod5 files
        self.pod5_files = self.run.get_unbasecalled_pod5_files()

        # Working dir
        self.working_dir = self.run.basecalling_batches_dir / self.batch_id

        # Output files
        self.output_bam = self.working_dir / BATCH_BAM
        self.log_file = self.working_dir / BATCH_LOG
        self.pod5_manifest = self.working_dir / BATCH_MANIFEST
        self.slurm_id_file = self.working_dir / BATCH_JOB_ID
        self.script_file = self.working_dir / BATCH_SCRIPT
        self.done_file = self.working_dir / BATCH_DONE

        # Pod5 lock files
        self.pod5_lock_files = [self.run.basecalling_lock_files_dir / f"{pod5_file.name}.lock" for pod5_file in self.pod5_files]
        for lock_file in self.pod5_lock_files:
            lock_file.parent.mkdir(exist_ok=True, parents=True)
            lock_file.touch()

        # Pod5 done files
        self.pod5_done_files = [self.run.basecalling_done_files_dir / f"{pod5_file.name}.done" for pod5_file in self.pod5_files]

    def setup(self):

        # Create working directory
        self.working_dir.mkdir(exist_ok=True, parents=True)

        # Write pod5 manifest
        pod5_files_str = "\n".join([str(x) for x in self.pod5_files]) + "\n"
        self.pod5_manifest.write_text(pod5_files_str, encoding="utf-8")

        # Create .lock files
        for lock_file in self.pod5_lock_files:
            lock_file.parent.mkdir(exist_ok=True, parents=True)
            lock_file.touch()


def cleanup_basecalling_lock_files(pod5_dir: SequencingRun):
    # Loop through all batch directories and collect inactive and active pod5 files
    queued_pod5_files: set[Path] = set()
    for batch_dir in pod5_dir.basecalling_batches_dir.glob("*"):

        # Skip if job is done
        if Path(batch_dir / BATCH_DONE).exists():
            continue

        # If job is in queue collect pod5 files and skip
        slurm_id_file = batch_dir / BATCH_JOB_ID
        pod5_manifest_file = batch_dir / BATCH_MANIFEST
        if slurm_id_file.exists() and pod5_manifest_file.exists():
            job_id = slurm_id_file.read_text().strip()
            if is_in_queue(job_id):
                pod5_files = read_pod5_manifest(pod5_manifest_file)
                queued_pod5_files.update(pod5_files)
                continue

    # Remove lock files for pod5 files that are not in active batch directories
    for lock_file in pod5_dir.get_lock_files():
        if lock_file.name not in [f"{x.name}.lock" for x in queued_pod5_files]:
            lock_file.unlink()


def read_pod5_manifest(pod5_manifest_file: Path) -> List[Path]:
    with open(pod5_manifest_file, "r", encoding="utf-8") as f:
        pod5_files = [Path(x.strip()) for x in f]
    return pod5_files


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


def process_unbasecalled_pod5_files(
    run: SequencingRun,
    min_batch_size: int,
    mail_user: str,
    dry_run: bool,
):

    batch = BasecallingBatch(run)

    if batch_should_be_skipped(batch, min_batch_size):
        logger.info(
            "Skipping. Too few pod5 files to basecall (%d<%d).",
            len(batch.pod5_files),
            min_batch_size,
        )
        return

    logger.info("Setting up basecalling batch (id: %s, %d pod5 files)", batch.batch_id, len(batch.pod5_files))
    batch.setup()

    submit_basecalling_batch_to_slurm(
        batch=batch,
        mail_user=mail_user,
        dry_run=dry_run,
    )


def batch_should_be_skipped(basecalling_batch: BasecallingBatch, min_batch_size: int) -> bool:

    # Skip if there are no pod5 files
    if not basecalling_batch.pod5_files:
        return True

    # Check if there are enough pod5 files to basecall
    if len(basecalling_batch.pod5_files) >= min_batch_size:
        return False

    # Check if unbasecalled pod5 files are the only ones left
    return not basecalling_batch.run.all_pod5_files_are_transferred()


def basecalling_is_pending(run: SequencingRun) -> bool:
    return run.dorado_config_file.exists() and has_unbasecalled_pod5_files(run)


def has_unbasecalled_pod5_files(pod5_dir: SequencingRun) -> bool:
    # Number of lock and done files
    lock_files = len(list(pod5_dir.get_lock_files()))
    done_files = len(list(pod5_dir.get_done_files()))

    # Number of pod5 files
    pod5_files = len(list(pod5_dir.get_transferred_pod5_files()))

    # If number of lock/done files is less than number of pod5 files, return True
    return lock_files + done_files < pod5_files


def submit_basecalling_batch_to_slurm(
    batch: BasecallingBatch,
    mail_user: str,
    dry_run: bool,
):

    # Get configuration
    dorado_executable = batch.run.config.dorado_executable
    basecalling_model = batch.run.config.basecalling_model
    modification_models = batch.run.config.modification_models

    # Convert path lists to strings
    pod5_files_str = " ".join([str(x) for x in batch.pod5_files])
    lock_files_str = " ".join([str(x) for x in batch.pod5_lock_files])
    done_files_str = " ".join([str(x) for x in batch.pod5_done_files])

    # Construct SLURM job script
    modified_bases_models_arg = ""
    modified_bases_models_str = ",".join([str(x) for x in modification_models])
    if modified_bases_models_str:
        modified_bases_models_arg = f"--modified-bases-models {modified_bases_models_str}"

    slurm_script = f"""\
#!/bin/bash
#SBATCH --account           MomaDiagnosticsHg38
#SBATCH --time              7-00:00:00
#SBATCH --cpus-per-task     2
#SBATCH --mem               32g
#SBATCH --partition         gpu
#SBATCH --gres              gpu:1
#SBATCH --mail-type         FAIL
#SBATCH --mail-user         {mail_user}
#SBATCH --output            {batch.script_file}.%j.out
#SBATCH --job-name          eldorado-basecalling
        
        set -eu
        # Trap all lock files
        LOCK_FILES_LIST=({lock_files_str})
        trap 'for LOCK_FILE in ${{LOCK_FILES_LIST[@]}}; do rm -f $LOCK_FILE; done' EXIT

        # Log start time
        START=$(date '+%Y-%m-%d %H:%M:%S')
        START_S=$(date '+%s')

        # Create temp bam on scratch
        TEMP_BAM_FILE="$TEMPDIR/out.bam"

        # Create pod5 dir on scratch
        POD5_DIR_TEMP="$TEMPDIR/pod5"
        mkdir -p $POD5_DIR_TEMP
        
        # Link pod5 files to scratch
        POD5_FILES_LIST=({pod5_files_str})
        for POD5_FILE in ${{POD5_FILES_LIST[@]}}
        do
            ln -s $POD5_FILE $POD5_DIR_TEMP
        done

        # Run basecaller
        {dorado_executable} basecaller \\
            --no-trim \\
            {modified_bases_models_arg} \\
            {basecalling_model} \\
            $POD5_DIR_TEMP \\
        > ${{TEMP_BAM_FILE}}

        # Move temp file to output 
        mkdir -p $(dirname {batch.output_bam})
        mv ${{TEMP_BAM_FILE}} {batch.output_bam}

        # Log end time
        END=$(date '+%Y-%m-%d %H:%M:%S')
        END_S=$(date +%s)
        RUNTIME=$((END_S-START_S))

        # Get size of input and output
        POD5_SIZE=$(du -sL {pod5_files_str} | cut -f1)
        POD5_COUNT=$(ls -1 {pod5_files_str} | grep ".pod5" | wc -l)
        OUTPUT_BAM_SIZE=$(du -sL {batch.output_bam} | cut -f1)
        BAM_READ_COUNT=$(samtools view -c {batch.output_bam})

        # Write log file
        LOG_FILE={batch.log_file}
        echo "slurm_job_id=$SLURM_JOB_ID" >> ${{LOG_FILE}}
        echo "pod5_size=$POD5_SIZE" >> ${{LOG_FILE}}
        echo "pod5_count=$POD5_COUNT" >> ${{LOG_FILE}}
        echo "output_bam={batch.output_bam}" >> ${{LOG_FILE}}
        echo "output_bam_size=$OUTPUT_BAM_SIZE" >> ${{LOG_FILE}}
        echo "bam_read_count=$BAM_READ_COUNT" >> ${{LOG_FILE}}
        echo "start=$START" >> ${{LOG_FILE}}
        echo "end=$END" >> ${{LOG_FILE}}
        echo "runtime=$RUNTIME" >> ${{LOG_FILE}}
        echo "basecaller={dorado_executable}" >> ${{LOG_FILE}}
        echo "basecalling_model={basecalling_model}" >> ${{LOG_FILE}}
        echo "modified_bases_models={modified_bases_models_str}" >> ${{LOG_FILE}}

        # Touch done files
        mkdir -p $(dirname {batch.done_file})
        touch {batch.done_file}
        
        DONE_FILES_LIST=({done_files_str})
        for DONE_FILE in ${{DONE_FILES_LIST[@]}}
        do
            mkdir -p $(dirname $DONE_FILE)
            touch $DONE_FILE
        done    

    """

    # Write Slurm script to a file
    logger.info("Writing script to %s", str(batch.script_file))
    write_to_file(batch.script_file, slurm_script)

    if dry_run:
        logger.info("Dry run. Skipping submission of basecalling job.")
        return

    # Submit the job using Slurm
    std_out = subprocess.run(
        ["sbatch", "--parsable", str(batch.script_file)],
        capture_output=True,
        check=True,
    )

    # Write job ID to file
    job_id = std_out.stdout.decode("utf-8").strip()
    write_to_file(batch.slurm_id_file, job_id)

    logger.info("Submitted basecalling job to SLURM with job ID %s", job_id)
