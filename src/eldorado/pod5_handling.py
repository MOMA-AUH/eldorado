from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Generator

import time
import re

from eldorado.constants import MIN_TIME
from eldorado.configuration import Metadata, get_metadata, Config


@dataclass
class BasecallingRun:
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
        self.output_dir = self.pod5_dir.parent / (self.pod5_dir.name.replace("pod5", "bam") + "_eldorado")
        self.basecalling_summary = self.output_dir / "basecalling_summary.txt"

        # Dorado config
        self.dorado_config_file = self.output_dir / "dorado_config.json"

        # Basecalling
        self.basecalling_working_dir = self.output_dir / "basecalling"
        self.basecalling_batches_dir = self.basecalling_working_dir / "batches"
        self.basecalling_lock_files_dir = self.basecalling_working_dir / "lock_files"
        self.basecalling_done_files_dir = self.basecalling_working_dir / "done_files"

        # Merging
        self.merging_working_dir = self.output_dir / "merging"
        self.merged_bam = self.merging_working_dir / "merged.bam"
        self.merge_script_file = self.merging_working_dir / "merge_bams.sh"
        self.merge_job_id_file = self.merging_working_dir / "merge_job_id.txt"
        self.merge_lock_file = self.merging_working_dir / "merge.lock"
        self.merge_done_file = self.merging_working_dir / "merge.done"

        # Demultiplexing
        self.demux_working_dir = self.output_dir / "demultiplexing"
        self.demux_script_file = self.demux_working_dir / "demux.sh"
        self.demux_job_id_file = self.demux_working_dir / "demux_job_id.txt"
        self.demux_lock_file = self.demux_working_dir / "demux.lock"
        self.demux_done_file = self.demux_working_dir / "demux.done"

    def get_pod5_files(self) -> Generator[Path, None, None]:
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

    def all_pod5_files_transferred(self) -> bool:
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
        pod5_files = self.get_pod5_files()
        n_pod5_files_count = len(list(pod5_files))

        # If number of pod5 files is euqal to expected number of pod5 files basecalling is done
        return n_pod5_files_expected == n_pod5_files_count


def is_file_inactive(file: Path, min_time: int) -> bool:
    time_since_data_last_modified = time.time() - file.stat().st_mtime
    return time_since_data_last_modified > min_time


def find_sequencning_runs_for_processing(root_dir: Path, pattern: str) -> List[BasecallingRun]:

    # Get all pod5 directories that match the pattern
    pod5_dirs = get_pod5_dirs_from_pattern(root_dir, pattern)

    # Keep only pod5 directories that are not already basecalled
    pod5_dirs = [x for x in pod5_dirs if needs_basecalling(x)]

    # Keep only pod5 directories that has pod5 files
    pod5_dirs = [x for x in pod5_dirs if contains_pod5_files(x)]

    # Return as Pod5Directory objects
    return [BasecallingRun(p) for p in pod5_dirs]


def get_pod5_dirs_from_pattern(root_dir: Path, pattern: str) -> List[Path]:
    return list(root_dir.glob(pattern=pattern))


def needs_basecalling(pod5_dir: Path) -> bool:
    return not any(pod5_dir.parent.glob("bam*/*.bam")) and not any(pod5_dir.parent.glob("fastq*/*.fastq*"))


def contains_pod5_files(x: Path) -> bool:
    return any(x.glob("*.pod5"))
