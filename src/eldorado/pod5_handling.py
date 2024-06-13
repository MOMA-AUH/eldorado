from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Generator

import time
import re

from eldorado.constants import MIN_TIME
from eldorado.configuration import Metadata, get_metadata, Config
import eldorado.filenames as fn


@dataclass
class SequencingRun:
    # Input attributes
    pod5_dir: Path

    # Derived attributes
    # Dorado config
    dorado_config_file: Path = field(init=False)

    # General
    output_dir: Path = field(init=False)
    basecalling_summary: Path = field(init=False)

    # Basecalling
    basecalling_working_dir: Path = field(init=False)
    basecalling_batches_dir: Path = field(init=False)
    basecalling_lock_files_dir: Path = field(init=False)
    basecalling_done_files_dir: Path = field(init=False)

    # Merging
    merging_working_dir: Path = field(init=False)
    merged_bam: Path = field(init=False)
    merge_script_file: Path = field(init=False)
    merge_job_id_file: Path = field(init=False)
    merge_lock_file: Path = field(init=False)
    merge_done_file: Path = field(init=False)

    # Demultiplexing
    demux_working_dir: Path = field(init=False)
    demux_script_file: Path = field(init=False)
    demux_job_id_file: Path = field(init=False)
    demux_lock_file: Path = field(init=False)
    demux_done_file: Path = field(init=False)

    # Metadata
    _metadata: Metadata = field(init=False)

    @property
    def metadata(self) -> Metadata:
        if not hasattr(self, "_metadata"):
            self._metadata = get_metadata(self.pod5_dir)
        return self._metadata

    # Dorado config
    _config: Config = field(init=False)

    @property
    def config(self) -> Config:
        if not hasattr(self, "_config"):
            self._config = Config.load(self.dorado_config_file)
        return self._config

    def __post_init__(self):
        # General
        self.output_dir = self.pod5_dir.parent / (self.pod5_dir.name.replace("pod5", "bam") + fn.OUTPUT_DIR_SUFFIX)
        self.basecalling_summary = self.output_dir / fn.BASECALLING_SUMMARY

        # Dorado config
        self.dorado_config_file = self.output_dir / fn.DORADO_CONFIG

        # Basecalling
        self.basecalling_working_dir = self.output_dir / fn.BC_DIR
        self.basecalling_batches_dir = self.basecalling_working_dir / fn.BC_BATCHES_DIR
        self.basecalling_lock_files_dir = self.basecalling_working_dir / fn.BC_LOCK_DIR
        self.basecalling_done_files_dir = self.basecalling_working_dir / fn.BC_DONE_DIR

        # Merging
        self.merging_working_dir = self.output_dir / fn.MERGE_DIR
        self.merged_bam = self.merging_working_dir / fn.MERGE_BAM
        self.merge_script_file = self.merging_working_dir / fn.MERGE_SCRIPT
        self.merge_job_id_file = self.merging_working_dir / fn.MERGE_JOB_ID
        self.merge_lock_file = self.merging_working_dir / fn.MERGE_LOCK
        self.merge_done_file = self.merging_working_dir / fn.MERGE_DONE

        # Demultiplexing
        self.demux_working_dir = self.output_dir / fn.DEMUX_DIR
        self.demux_script_file = self.demux_working_dir / fn.DEMUX_SCRIPT
        self.demux_job_id_file = self.demux_working_dir / fn.DEMUX_JOB_ID
        self.demux_lock_file = self.demux_working_dir / fn.DEMUX_LOCK
        self.demux_done_file = self.demux_working_dir / fn.DEMUX_DONE

    def get_transferred_pod5_files(self) -> Generator[Path, None, None]:
        # Get all pod5 files
        pod5_files = self.pod5_dir.glob("*.pod5")

        # Return only files that have been inactive for min_time
        return (pod5 for pod5 in pod5_files if is_file_inactive(pod5, MIN_TIME))

    def get_lock_files(self) -> List[Path]:
        return list(self.basecalling_lock_files_dir.glob("*.lock"))

    def get_done_files(self) -> List[Path]:
        return list(self.basecalling_done_files_dir.glob("*.done"))

    def get_final_summary(self) -> Path | None:
        return next(self.pod5_dir.parent.glob("final_summary*.txt"), None)

    def get_sample_sheet_path(self) -> Path | None:
        return next(self.pod5_dir.parent.glob("sample_sheet*.csv"), None)

    def all_pod5_files_are_transferred(self) -> bool:
        # Get final summary
        final_summary = self.get_final_summary()

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
            return False

        # Get expected number of pod5 files
        n_pod5_files_expected = int(matches[1])

        # Count the number of pod5 files
        pod5_files = self.get_transferred_pod5_files()
        n_pod5_files_count = len(list(pod5_files))

        # If number of pod5 files is euqal to expected number of pod5 files basecalling is done
        return n_pod5_files_expected == n_pod5_files_count

    def get_unbasecalled_pod5_files(self):
        pod5_files = self.get_transferred_pod5_files()

        lock_files_names = [lock_file.name for lock_file in self.get_lock_files()]
        done_files_names = [done_file.name for done_file in self.get_done_files()]

        return [pod5 for pod5 in pod5_files if f"{pod5.name}.lock" not in lock_files_names and f"{pod5.name}.done" not in done_files_names]


def is_file_inactive(file: Path, min_time: int) -> bool:
    time_since_data_last_modified = time.time() - file.stat().st_mtime
    return time_since_data_last_modified > min_time


def find_sequencning_runs_for_processing(root_dir: Path, pattern: str) -> List[SequencingRun]:

    # Get all pod5 directories that match the pattern
    pod5_dirs = get_pod5_dirs_from_pattern(root_dir, pattern)

    # Keep only pod5 directories that are not already basecalled
    pod5_dirs = [x for x in pod5_dirs if needs_basecalling(x)]

    # Keep only pod5 directories that has pod5 files
    pod5_dirs = [x for x in pod5_dirs if contains_pod5_files(x)]

    # Return as Pod5Directory objects
    return [SequencingRun(p) for p in pod5_dirs]


def get_pod5_dirs_from_pattern(root_dir: Path, pattern: str) -> List[Path]:
    return list(root_dir.glob(pattern=pattern))


def needs_basecalling(pod5_dir: Path) -> bool:
    # Get the prefix for the bam and fastq directories i.e. pod5_pass -> bam_pass, fastq_pass
    bam_dir_prefix = pod5_dir.name.replace("pod5", "bam")
    fastq_dir_prefix = pod5_dir.name.replace("pod5", "fastq")

    # Check if any bam or fastq files already exist
    any_existing_bam_files = any(pod5_dir.parent.glob(f"{bam_dir_prefix}*/*.bam"))
    any_existing_fastq_files = any(pod5_dir.parent.glob(f"{fastq_dir_prefix}*/*.fastq*"))

    return not any_existing_bam_files and not any_existing_fastq_files


def contains_pod5_files(x: Path) -> bool:
    return any(x.glob("*.pod5"))
