from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import time
import re

import pod5

from eldorado.constants import MIN_TIME


@dataclass
class BasecallingBatch:
    pod5_files: List[Path]
    batches_dir: Path
    pod5_lock_files_dir: Path
    pod5_done_files_dir: Path

    # Derived attributes
    batch_id: str = field(init=False)

    batch_dir: Path = field(init=False)
    bam: Path = field(init=False)
    pod5_manifest: Path = field(init=False)
    slurm_id_txt: Path = field(init=False)
    script_file: Path = field(init=False)

    done_file = Path
    pod5_lock_files: List[Path] = field(init=False)
    pod5_done_files: List[Path] = field(init=False)

    def __post_init__(self):
        # Create batch id from current time
        self.batch_id = str(int(time.time()))

        # Output files
        self.batch_dir = self.batches_dir / self.batch_id
        self.bam = self.batch_dir / f"basecalled_batch_{self.batch_id}.bam"
        self.pod5_manifest = self.batch_dir / "pod5_manifest.txt"
        self.slurm_id_txt = self.batch_dir / "slurm_id.txt"

        self.script_file = self.batch_dir / f"run_basecaller_batch_{self.batch_id}.sh"

        # Lock files
        self.pod5_lock_files = [self.pod5_lock_files_dir / f"{pod5_file.name}.lock" for pod5_file in self.pod5_files]

        # Done files
        self.done_file = self.batch_dir / "batch.done"
        self.pod5_done_files = [self.pod5_done_files_dir / f"{pod5_file.name}.done" for pod5_file in self.pod5_files]


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
    final_bam: Path = field(init=False)

    # Basecalling
    basecalling_working_dir: Path = field(init=False)
    bam_batches_dir: Path = field(init=False)
    basecalling_lock_files_dir: Path = field(init=False)
    basecalling_done_files_dir: Path = field(init=False)

    # Merging
    merging_working_dir: Path = field(init=False)
    merged_bam: Path = field(init=False)
    merge_script_file: Path = field(init=False)
    merge_log_file: Path = field(init=False)
    merge_job_id_file: Path = field(init=False)
    merge_lock_file: Path = field(init=False)
    merge_done_file: Path = field(init=False)
    merge_pod5_manifest: Path = field(init=False)

    def __post_init__(self):
        # General
        self.output_dir = self.path.parent / (self.path.name.replace("pod5", "bam") + "_eldorado")
        self.script_dir = self.output_dir / "basecall_scripts"
        self.final_bam = self.output_dir / "basecalled.bam"

        # Basecalling
        self.basecalling_working_dir = self.output_dir / "basecalling"
        self.bam_batches_dir = self.basecalling_working_dir / "batches"
        self.basecalling_lock_files_dir = self.basecalling_working_dir / "lock_files"
        self.basecalling_done_files_dir = self.basecalling_working_dir / "done_files"

        # Merging
        self.merging_working_dir = self.output_dir / "merging"
        self.merged_bam = self.merging_working_dir / "merged.bam"
        self.merge_script_file = self.merging_working_dir / "merge_bams.sh"
        self.merge_log_file = self.merging_working_dir / "merge.log"
        self.merge_job_id_file = self.merging_working_dir / "merge_job_id.txt"
        self.merge_lock_file = self.merging_working_dir / "merge.lock"
        self.merge_done_file = self.merging_working_dir / "merge.done"
        self.merge_pod5_manifest = self.merging_working_dir / "pod5_manifest.txt"

    def get_pod5_files(self) -> List[Path]:
        # Get all pod5 files
        pod5_files = self.path.glob("*.pod5")

        # Return only files that have been inactive for min_time
        return [pod5 for pod5 in pod5_files if is_file_inactive(pod5, MIN_TIME)]

    def get_lock_files(self) -> List[Path]:
        return list(self.basecalling_lock_files_dir.glob("*.lock"))

    def get_done_files(self) -> List[Path]:
        return list(self.basecalling_done_files_dir.glob("*.done"))

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

    def all_pod5_files_transfered(self) -> bool:
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
