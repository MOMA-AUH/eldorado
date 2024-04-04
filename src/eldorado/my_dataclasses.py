from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import pod5


@dataclass
class SequencingRun:
    # Data directory
    pod5_dir: Path

    # Sample metadata
    project_id: str
    sample_id: str
    protocol_run_id: str

    # Sequencing metadata
    sample_rate: float
    flow_cell_product_code: str
    sequencing_kit: str

    # Derived attributes
    output_dir: Path = field(init=False)
    output_bam: Path = field(init=False)
    output_bam_parts_dir: Path = field(init=False)
    script_dir: Path = field(init=False)
    pod5_files_locked_dir: Path = field(init=False)
    pod5_files_done_dir: Path = field(init=False)

    def __post_init__(self):
        # Output paths
        self.output_dir = self.pod5_dir.parent / (self.pod5_dir.name.replace("pod5", "bam") + "_eldorado")
        self.output_bam = self.output_dir / "basecalled.bam"
        self.output_bam_parts_dir = self.output_dir / "basecalled_parts"
        # Script names
        self.script_dir = self.output_bam.parent / "basecall_scripts"
        # Lock and done files
        self.pod5_files_locked_dir = self.output_bam.parent / "lock_files"
        self.pod5_files_done_dir = self.output_bam.parent / "done_files"

    def get_pod5_files(self) -> List[Path]:
        return list(self.pod5_dir.glob("*.pod5"))

    def get_pod5_lock_files(self) -> List[Path]:
        return list(self.pod5_files_locked_dir.glob("*.lock"))

    def lock_files_from_list(self, pod5_files: List[Path]) -> List[Path]:
        return [self.pod5_files_locked_dir / f"{pod5_file.name}.lock" for pod5_file in pod5_files]

    def get_pod5_done_files(self) -> List[Path]:
        return list(self.pod5_files_done_dir.glob("*.done"))

    @classmethod
    def create_from_pod5_dir(cls, pod5_dir: Path):
        # Get first pod5 file
        pod5_files = pod5_dir.glob("*.pod5")
        first_pod5_file = next(pod5_files, None)

        # Check if pod5 files exist
        if first_pod5_file is None:
            raise ValueError(f"No pod5 files found in {pod5_dir}")

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

        # Create sequencing run
        return cls(
            pod5_dir=pod5_dir,
            project_id=experiment_name,
            sample_id=sample_id,
            protocol_run_id=protocol_run_id,
            sample_rate=sample_rate,
            flow_cell_product_code=flow_cell_product_code,
            sequencing_kit=sequencing_kit,
        )
