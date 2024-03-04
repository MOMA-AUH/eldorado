from dataclasses import dataclass
from pathlib import Path

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

    def get_script_name_basecall(self) -> str:
        return "dorado_basecalling_" + self.project_id + "_" + self.sample_id + "_" + self.protocol_run_id + ".sh"

    def get_script_name_demux(self) -> str:
        return "dorado_demux_" + self.project_id + "_" + self.sample_id + "_" + self.protocol_run_id + ".sh"

    def lock_file_basecall(self) -> Path:
        return self.pod5_dir.parent / (self.pod5_dir.name + ".lock")

    def lock_file_demux(self) -> Path:
        return self.output_bam().parent / (self.output_bam().name + ".lock")

    def done_file_demux(self) -> Path:
        return self.output_bam().parent / (self.output_bam().name + ".done")

    def output_bam(self) -> Path:
        return self.pod5_dir.parent / (self.pod5_dir.name.replace("pod5", "bam") + "_eldorado") / ("basecalled.bam")

    def output_dir_demux(self) -> Path:
        return self.output_bam().parent

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
        project_id = run_info.experiment_name
        sample_id = run_info.sample_id
        protocol_run_id = run_info.protocol_run_id

        # Sequencing metadata
        sample_rate = run_info.sample_rate
        flow_cell_product_code = run_info.flow_cell_product_code
        sequencing_kit = run_info.sequencing_kit

        # Create sequencing run
        return cls(
            pod5_dir=pod5_dir,
            project_id=project_id,
            sample_id=sample_id,
            protocol_run_id=protocol_run_id,
            sample_rate=sample_rate,
            flow_cell_product_code=flow_cell_product_code,
            sequencing_kit=sequencing_kit,
        )
