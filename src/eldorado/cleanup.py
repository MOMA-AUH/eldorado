from typing import List, Generator

import subprocess
import csv

from pathlib import Path

from eldorado.pod5_handling import SequencingRun
from eldorado.merging import get_done_batch_dirs
from eldorado.filenames import BATCH_LOG
from eldorado.logging_config import logger


def needs_cleanup(run: SequencingRun) -> bool:
    return run.demux_done_file.exists() and any(run.output_dir.glob("*.bam"))


def cleanup_output_dir(run: SequencingRun) -> None:

    # Concatenate batch log files from done batches to a single csv file
    batch_dirs = get_done_batch_dirs(run)
    log_files = [log_file for batch_dir in batch_dirs for log_file in batch_dir.glob(BATCH_LOG)]
    log_dicts = load_logs_as_dicts(log_files)
    generate_final_log_csv(run.basecalling_summary, log_dicts)
    logger.info("Generated final log CSV file %s", run.basecalling_summary)

    # Clean up of basecalling
    subprocess.run(["rm", "-rf", str(run.basecalling_working_dir)], check=True)

    # Clean up of merging
    subprocess.run(["rm", "-rf", str(run.merging_working_dir)], check=True)

    # Clean up of demultiplexing
    subprocess.run(["rm", "-rf", str(run.demux_working_dir)], check=True)

    logger.info("Removed working directories")


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
