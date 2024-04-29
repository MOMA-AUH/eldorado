from typing import List, Generator

import subprocess
import csv

from pathlib import Path

from eldorado.pod5_handling import BasecallingRun
from eldorado.logging_config import logger


def needs_cleanup(run: BasecallingRun) -> bool:
    return run.demux_done_file.exists() and any(run.output_dir.glob("*.bam"))


def cleanup_output_dir(pod5_dir: BasecallingRun) -> None:

    # Remove all batch that have not been used for the final BAM file
    cleanup_stalled_basecalling_dirs(pod5_dir)

    # Concatenate all batch log files to a single csv file
    log_files = pod5_dir.basecalling_batches_dir.glob("*/basecalled.log")
    dict_list = load_logs_as_dicts(log_files)
    generate_final_log_csv(pod5_dir.basecalling_summary, dict_list)

    # Clean up of basecalling
    subprocess.run(["rm", "-rf", str(pod5_dir.basecalling_working_dir)], check=True)

    # Clean up of merging
    subprocess.run(["rm", "-rf", str(pod5_dir.merging_working_dir)], check=True)

    # Clean up of demultiplexing
    subprocess.run(["rm", "-rf", str(pod5_dir.demux_working_dir)], check=True)


def generate_final_log_csv(csv_file: Path, logs: List[dict]):
    # Get all unique keys from all log dictionaries (preserve order)
    header = [*dict.fromkeys(key for log_dict in logs for key in log_dict.keys())]
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(logs)


def load_logs_as_dicts(log_files: List[Path] | Generator[Path, None, None]):
    dict_list = []
    for log_file in log_files:
        # Convert log file to dict
        with open(log_file, "r", encoding="utf-8") as f:
            log_lines = f.read().splitlines()
            log_dict = {}
            for line in log_lines:
                key, value = line.split("=")
                log_dict[key] = value
            dict_list.append(log_dict)
    return dict_list


def cleanup_stalled_basecalling_dirs(pod5_dir: BasecallingRun):

    stalled_batch_dirs: set[Path] = {batch_dir for batch_dir in pod5_dir.basecalling_batches_dir.glob("*") if not (batch_dir / "batch.done").exists()}

    for batch_dir in stalled_batch_dirs:
        logger.info("Removing stalled batch directory %s", str(batch_dir))
        subprocess.run(["rm", "-rf", str(batch_dir)], check=True)
