from pathlib import Path
from typing import List
import re
import json

from dataclasses import dataclass

import pod5

from eldorado.constants import MODELS_DIR


@dataclass
class Metadata:
    project_id: str
    library_pool_id: str
    protocol_run_id: str
    sample_rate: int
    flow_cell_product_code: str
    sequencing_kit: str


@dataclass
class Config:
    dorado_executable: Path
    basecalling_model: Path
    modification_models: List[Path]

    def save(self, path: Path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "DORADO_EXECUTABLE": str(self.dorado_executable),
                    "BASECALLING_MODEL": str(self.basecalling_model),
                    "MODIFICATION_MODELS": [str(x) for x in self.modification_models],
                },
                f,
                indent=4,
            )

    @classmethod
    def load(cls, path: Path):
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)

        return cls(
            dorado_executable=Path(config["DORADO_EXECUTABLE"]),
            basecalling_model=Path(config["BASECALLING_MODEL"]),
            modification_models=[Path(x) for x in config["MODIFICATION_MODELS"]],
        )


def get_metadata(pod5_dir: Path) -> Metadata:
    # Get all pod5 files
    pod5_files = pod5_dir.glob("*.pod5")

    # Get first pod5 file
    first_pod5_file = next(pod5_files, None)

    # If no pod5 files are found raise error
    if first_pod5_file is None:
        raise FileNotFoundError(f"No pod5 files found in directory {pod5_dir}")

    # Get first read from file
    first_pod5_read = next(pod5.Reader(first_pod5_file).reads())

    # Unpack run info
    run_info = first_pod5_read.run_info

    # Set metadata
    return Metadata(
        project_id=run_info.experiment_name,  # We call it project_id
        library_pool_id=run_info.sample_id,  # We call it library_id
        protocol_run_id=run_info.protocol_run_id,
        sample_rate=run_info.sample_rate,
        flow_cell_product_code=run_info.flow_cell_product_code.upper(),
        sequencing_kit=run_info.sequencing_kit.upper(),
    )


def get_dorado_config(
    metadata: Metadata,
    modifications: List[str],
    dorado_executable: Path,
) -> Config:
    # Get relevant models
    basecalling_model = get_basecalling_model(metadata)
    modification_models = get_modified_bases_models(basecalling_model.name, modifications)

    return Config(
        dorado_executable=dorado_executable,
        basecalling_model=basecalling_model,
        modification_models=modification_models,
    )


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


def get_basecalling_model(metadata: Metadata) -> Path:

    # Find basecalling model
    # Link to model documentation: https://github.com/nanoporetech/dorado?tab=readme-ov-file#dna-models
    basecalling_model = None

    # If run on FLO-PRO114M (R10.4.1) flow cell
    if metadata.flow_cell_product_code == "FLO-PRO114M":
        if metadata.sample_rate == 4000:
            basecalling_model = MODELS_DIR / "dna_r10.4.1_e8.2_400bps_hac@v4.1.0"
        elif metadata.sample_rate == 5000:
            basecalling_model = MODELS_DIR / "dna_r10.4.1_e8.2_400bps_hac@v4.3.0"

    # If run on FLO-PRO002 (R9.4.1) flow cell
    elif metadata.flow_cell_product_code == "FLO-PRO002":
        basecalling_model = MODELS_DIR / "dna_r9.4.1_e8_hac@v3.3"

    # Check if basecalling model was found
    if basecalling_model is None:
        raise ValueError(f"Could not find basecalling model. Metadata: {str(metadata)}")
    # Check if basecalling model exists
    if not basecalling_model.exists():
        raise ValueError(f"Basecalling model {basecalling_model} does not exist")

    return basecalling_model


def get_modified_bases_models(
    basecalling_model: str,
    modifications: List[str],
    models_dir: Path = MODELS_DIR,
) -> List[Path]:

    modified_bases_models = []

    # Get all modified base models based on base model
    for mod in modifications:
        # If more than one model found, select the latest one and add it to the list
        if mod_models := list(models_dir.glob(f"{basecalling_model}*{mod}*")):
            latest_mod_model = get_latest_version(mod_models)
            modified_bases_models.append(latest_mod_model)

    return modified_bases_models
