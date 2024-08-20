from pathlib import Path
from typing import List, Tuple
import re
import json

import csv

from dataclasses import dataclass

import pod5

from eldorado.constants import (
    REQUIRED_PROJECT_CONFIG_FIELDS,
    DEFAULT_PROJECT_NAME,
    PROJECT_ID,
    DORADO_EXECUTABLE,
    BASECALLING_MODEL,
    MOD_5MCG_5HMCG,
    MOD_6MA,
)
from eldorado.logging_config import logger
from eldorado.utils import write_to_file, is_complete_pod5_file


@dataclass
class Metadata:
    project_id: str
    library_pool_id: str
    protocol_run_id: str
    sample_rate: int
    flow_cell_product_code: str
    sequencing_kit: str


@dataclass
class ProjectConfig:
    project_id: str
    dorado_executable: Path
    basecalling_model: Path | None  # None: Auto select model
    mod_5mcg_5hmcg: bool
    mod_6ma: bool


@dataclass
class Config:
    dorado_executable: Path
    basecalling_model: Path
    modification_models: List[Path]

    def save(self, path: Path):
        content = json.dumps(
            {
                "dorado_executable": str(self.dorado_executable),
                "basecalling_model": str(self.basecalling_model),
                "modification_models": [str(x) for x in self.modification_models],
            },
            indent=4,
        )
        write_to_file(path, content)

    @classmethod
    def load(cls, path: Path):
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)

        return cls(
            dorado_executable=Path(config["dorado_executable"]),
            basecalling_model=Path(config["basecalling_model"]),
            modification_models=[Path(x) for x in config["modification_models"]],
        )


def get_metadata(pod5_dir: Path) -> Metadata:
    # Get all pod5 files
    pod5_files = pod5_dir.glob("*.pod5")

    # Get first complete pod5 file
    first_complete_pod5_file = next((pod5 for pod5 in pod5_files if is_complete_pod5_file(pod5)), None)

    # If no pod5 files are found raise error
    if first_complete_pod5_file is None:
        raise FileNotFoundError(f"No pod5 files found in directory {pod5_dir}")

    # Get first read from file
    first_pod5_read = next(pod5.Reader(first_complete_pod5_file).reads())

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


def get_default_project_config(row_dicts: List[dict]) -> ProjectConfig:

    for row in row_dicts:
        if row["project_id"] == DEFAULT_PROJECT_NAME:
            # Unpack row
            project_id, dorado_executable, basecalling_model, mod_5mcg_5hmcg, mod_6ma = unpack_config_row(row)

            # Basecalling model
            basecalling_model = None if basecalling_model == "auto" else Path(basecalling_model)

            # Return default project config
            return ProjectConfig(
                project_id,
                Path(dorado_executable),
                basecalling_model,
                bool(int(mod_5mcg_5hmcg)),
                bool(int(mod_6ma)),
            )

    logger.error("Default project %s not found in project configs file!", DEFAULT_PROJECT_NAME)
    raise ValueError(f"Default project {DEFAULT_PROJECT_NAME} not found in project configs file!")


def get_project_configs(
    csv_file: Path,
) -> List[ProjectConfig]:

    # Load csv file and strip whitespace from keys and values
    with open(csv_file, "r", encoding="utf-8") as f:
        dict_reader = csv.DictReader(f, delimiter=",")
        row_dicts = [{key.strip(): value.strip() if value else "" for key, value in d.items()} for d in dict_reader]

    # Check if config file is empty
    if not row_dicts:
        logger.error("No project configs found in project configs file.")
        return []

    # Check if any required fields are missing
    fieldnames = list(row_dicts[0].keys())
    invalid_header = any(field not in fieldnames for field in REQUIRED_PROJECT_CONFIG_FIELDS)
    if invalid_header:
        missing_fields = [field for field in REQUIRED_PROJECT_CONFIG_FIELDS if field not in fieldnames]
        logger.error("Required fields %s are missing in project configs file.", missing_fields)
        return []

    # Filter out invalid rows
    row_dicts = [row for row in row_dicts if is_row_inputs_valid(row)]

    # Check if default project is missing
    project_ids = [d[PROJECT_ID] for d in row_dicts]
    if DEFAULT_PROJECT_NAME not in project_ids:
        logger.error("Default project is missing from project configs file")
        return []

    # Check for duplicate project IDs
    if len(project_ids) != len(set(project_ids)):
        duplicate_project_ids = [project_id for project_id in project_ids if project_ids.count(project_id) > 1]
        logger.error("Duplicate project IDs found in project configs file: %s", duplicate_project_ids)
        return []

    default_project_config = get_default_project_config(row_dicts)

    project_configs_dict = []
    for row in row_dicts:
        # Skip default project
        if row[PROJECT_ID] == DEFAULT_PROJECT_NAME:
            continue

        # Unpack row
        project_config = get_config_from_row(row, default_project_config)

        # Add to dict
        project_configs_dict.append(project_config)

    return project_configs_dict


def unpack_config_row(row: dict) -> Tuple[str, str, str, str, str]:
    project_id = row[PROJECT_ID]
    dorado_executable = row[DORADO_EXECUTABLE]
    basecalling_model = row[BASECALLING_MODEL]
    mod_5mcg_5hmcg = row[MOD_5MCG_5HMCG]
    mod_6ma = row[MOD_6MA]

    return project_id, dorado_executable, basecalling_model, mod_5mcg_5hmcg, mod_6ma


def get_config_from_row(row: dict, project_defaults: ProjectConfig) -> ProjectConfig:

    # Unpack row
    project_id, dorado_executable_input, basecalling_model_input, mod_5mcg_5hmcg_input, mod_6ma_input = unpack_config_row(row)

    # Parse inputs, use defaults if not provided
    dorado_executable = Path(dorado_executable_input) if dorado_executable_input else project_defaults.dorado_executable
    mod_5mcg_5hmcg = bool(int(mod_5mcg_5hmcg_input)) if mod_5mcg_5hmcg_input else project_defaults.mod_5mcg_5hmcg
    mod_6ma = bool(int(mod_6ma_input)) if mod_6ma_input else project_defaults.mod_6ma

    # Basecalling model: Set to None if "auto". This will be resolved later per metadata
    basecalling_model = (
        None
        if basecalling_model_input == "auto"
        else Path(basecalling_model_input) if basecalling_model_input else project_defaults.basecalling_model
    )

    # Return project config
    return ProjectConfig(
        project_id,
        dorado_executable,
        basecalling_model,
        mod_5mcg_5hmcg,
        mod_6ma,
    )


def is_row_inputs_valid(
    row: dict[str, str],
) -> bool:

    project_id_input, dorado_executable_input, basecalling_model_input, mod_5mcg_5hmcg_input, mod_6ma_input = unpack_config_row(row)

    is_default = project_id_input == DEFAULT_PROJECT_NAME

    if not project_id_input:
        return False

    if is_default or dorado_executable_input:
        dorado_executable_path = Path(dorado_executable_input)
        if not is_dorado_executable_valid(dorado_executable_path):
            logger.error("Dorado executable %s does not exist or is invalid for project %s", dorado_executable_input, project_id_input)
            return False

    if is_default or basecalling_model_input:
        basecalling_model_path = Path(basecalling_model_input)
        if basecalling_model_input != "auto" and not is_basecalling_model_path_valid(basecalling_model_path):
            logger.error("Basecalling model %s for project %s is not 'auto' or a valid path", basecalling_model_input, project_id_input)
            return False

    for field, field_name in zip([mod_5mcg_5hmcg_input, mod_6ma_input], [MOD_5MCG_5HMCG, MOD_6MA]):
        if (is_default or field) and field not in ["0", "1"]:
            logger.error("Field %s must be either 0 or 1 (not %s) for project %s", field_name, field, project_id_input)
            return False

    return True


def is_basecalling_model_path_valid(basecalling_model_path: Path):
    return basecalling_model_path.is_dir()


def is_dorado_executable_valid(dorado_executable_path: Path):
    return dorado_executable_path.is_file() and dorado_executable_path.name == "dorado"


def get_dorado_config(
    metadata: Metadata,
    dorado_executable: Path,
    basecalling_model: Path | None,
    mod_5mcg_5hmcg: bool,
    mod_6ma: bool,
    models_dir: Path,
) -> Config:

    # Get relevant model
    basecalling_model = basecalling_model if basecalling_model is not None else get_basecalling_model(metadata, models_dir)

    # Get modified base models
    modification_models = get_modification_models(
        basecalling_model,
        mod_5mcg_5hmcg,
        mod_6ma,
    )

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


def get_basecalling_model(metadata: Metadata, models_dir: Path) -> Path:

    # Find basecalling model
    # Link to model documentation: https://github.com/nanoporetech/dorado?tab=readme-ov-file#dna-models
    basecalling_model = None

    # If run on FLO-PRO114M (R10.4.1) flow cell
    if metadata.flow_cell_product_code == "FLO-PRO114M":
        if metadata.sample_rate == 4000:
            basecalling_model = models_dir / "dna_r10.4.1_e8.2_400bps_hac@v4.1.0"
        elif metadata.sample_rate == 5000:
            basecalling_model = models_dir / "dna_r10.4.1_e8.2_400bps_hac@v4.3.0"

    # If run on FLO-PRO002 (R9.4.1) flow cell
    elif metadata.flow_cell_product_code == "FLO-PRO002":
        basecalling_model = models_dir / "dna_r9.4.1_e8_hac@v3.3"

    # Check if basecalling model was found
    if basecalling_model is None:
        raise ValueError(f"Could not find basecalling model. Metadata: {str(metadata)}")
    # Check if basecalling model exists
    if not basecalling_model.exists():
        raise ValueError(f"Basecalling model {basecalling_model} does not exist")

    return basecalling_model


def get_modification_models(
    basecalling_model: Path,
    mod_5mcg_5hmcg: bool,
    mod_6ma: bool,
) -> List[Path]:

    # Create list of modifications
    modifications = []
    if mod_5mcg_5hmcg:
        modifications.append("5mCG_5hmCG")
    if mod_6ma:
        modifications.append("6mA")

    # Get all modified base models based on base model
    modified_bases_models = []
    for mod in modifications:
        # If more than one model found, select the latest one and add it to the list
        if mod_models := list(basecalling_model.parent.glob(f"{basecalling_model.name}*{mod}*")):
            latest_mod_model = get_latest_version(mod_models)
            modified_bases_models.append(latest_mod_model)

    return modified_bases_models
