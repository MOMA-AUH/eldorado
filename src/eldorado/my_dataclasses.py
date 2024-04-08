from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import time
import re

import pod5


@dataclass
class RunMetadata:
    project_id: str
    sample_id: str
    protocol_run_id: str
    sample_rate: int
    flow_cell_product_code: str
    sequencing_kit: str


@dataclass
class Pod5Directory:
    # Data directory
    path: Path

    # Derived attributes
    # General
    output_dir: Path = field(init=False)
    script_dir: Path = field(init=False)
    # Basecalling
    output_bam: Path = field(init=False)
    output_bam_parts_dir: Path = field(init=False)
    basecalling_lock_files_dir: Path = field(init=False)
    basecalling_dine_files_dir: Path = field(init=False)
    # Merging
    merge_lock_file: Path = field(init=False)

    def __post_init__(self):
        # General
        self.output_dir = self.path.parent / (self.path.name.replace("pod5", "bam") + "_eldorado")
        self.script_dir = self.output_dir / "basecall_scripts"
        # Basecalling
        self.output_bam = self.output_dir / "basecalled.bam"
        self.output_bam_parts_dir = self.output_dir / "basecalled_parts"
        self.basecalling_lock_files_dir = self.output_dir / "lock_files"
        self.basecalling_dine_files_dir = self.output_dir / "done_files"
        # Merging
        self.merge_lock_file = self.output_dir / "merge.lock"

    def get_pod5_files(self) -> List[Path]:
        return list(self.path.glob("*.pod5"))

    def get_pod5_lock_files(self) -> List[str]:
        return [x.name for x in self.basecalling_lock_files_dir.glob("*.pod5.lock")]

    def lock_files_from_list(self, pod5_files: List[Path]) -> List[Path]:
        return [self.basecalling_lock_files_dir / f"{pod5_file.name}.lock" for pod5_file in pod5_files]

    def done_files_from_list(self, pod5_files: List[Path]) -> List[Path]:
        return [self.basecalling_dine_files_dir / f"{pod5_file.name}.done" for pod5_file in pod5_files]

    def get_pod5_done_files(self) -> List[str]:
        return [x.name for x in self.basecalling_dine_files_dir.glob("*.pod5.done")]

    def get_pod5_files_for_basecalling(self):
        pod5_files = self.get_pod5_files()

        # Filter pod5 files for which there is a lock file or a done file
        pod5_files = [pod5 for pod5 in pod5_files if f"{pod5.name}.lock" not in self.get_pod5_lock_files()]
        pod5_files = [pod5 for pod5 in pod5_files if f"{pod5.name}.done" not in self.get_pod5_done_files()]

        # Filter pod5 files for which the data is done transfering
        thirty_minutes_in_sec = 30 * 60
        pod5_files = [pod5 for pod5 in pod5_files if is_file_inactive(pod5, thirty_minutes_in_sec)]

        return pod5_files

    def get_final_summary(self) -> Path | None:
        return next(self.path.parent.glob("final_summary*.txt"), None)

    def get_run_metadata(self) -> RunMetadata:
        # Get first pod5 file
        pod5_files = self.path.glob("*.pod5")
        first_pod5_file = next(pod5_files, None)

        # Check if pod5 files exist
        if first_pod5_file is None:
            raise ValueError(f"No pod5 files found in {self.path}")

        # Get first read from file
        first_pod5_read = next(pod5.Reader(first_pod5_file).reads())

        # Unpack run info
        run_info = first_pod5_read.run_info

        # Sample metadata
        experiment_name = run_info.experiment_name
        sample_id = run_info.sample_id
        protocol_run_id = run_info.protocol_run_id

        # Sequencing metadata
        sample_rate = run_info.sample_rate
        flow_cell_product_code = run_info.flow_cell_product_code
        sequencing_kit = run_info.sequencing_kit

        # Output metadata
        return RunMetadata(
            project_id=experiment_name,
            sample_id=sample_id,
            protocol_run_id=protocol_run_id,
            sample_rate=sample_rate,
            flow_cell_product_code=flow_cell_product_code,
            sequencing_kit=sequencing_kit,
        )

    def are_pod5_all_files_transfered(self) -> bool:
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
        n_pod5_files_count = len(pod5_files)

        # If number of pod5 files is euqal to expected number of pod5 files basecalling is done
        return n_pod5_files_expected == n_pod5_files_count


def is_file_inactive(file: Path, min_time: int) -> bool:
    time_since_data_last_modified = time.time() - file.stat().st_mtime
    return time_since_data_last_modified > min_time
