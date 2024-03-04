import re
from pathlib import Path
from typing import List, Tuple

from eldorado.constants import MODELS_DIR, MODIFICATIONS
from eldorado.my_dataclasses import SequencingRun


def compare_versions(version1, version2):
    """
    Compare two version strings.

    Returns:
        True if version1 => version2
        FALSE if version1 < version2
    """
    v1_components = [int(x) for x in version1.split(".")]
    v2_components = [int(x) for x in version2.split(".")]

    # Pad the shorter version with zeros
    while len(v1_components) < len(v2_components):
        v1_components.append(0)
    while len(v2_components) < len(v1_components):
        v2_components.append(0)

    # Compare each component
    for v1_component, v2_component in zip(v1_components, v2_components):
        if v1_component > v2_component:
            return True
        elif v1_component < v2_component:
            return False

    # If all components are equal, return False
    return False


def extract_version(path: Path) -> str:
    pattern = r"@v([\.\d+]{0,3})$"
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
        if version:
            if latest_version is None or compare_versions(version, latest_version):
                latest_version = version
                latest_path = path

    return latest_path


def model_selector(run: SequencingRun) -> Tuple[Path, List[Path]]:

    # Find basecalling model
    # Link to model documentation: https://github.com/nanoporetech/dorado?tab=readme-ov-file#dna-models
    basecalling_model = None
    modified_bases_models = []
    # If run on FLO-PRO114M (R10.4.1) flow cell
    if run.flow_cell_product_code == "FLO-PRO114M":
        if run.sample_rate == 4000:
            basecalling_model = MODELS_DIR / "dna_r10.4.1_e8.2_400bps_hac@v4.1.0"
        elif run.sample_rate == 5000:
            basecalling_model = MODELS_DIR / "dna_r10.4.1_e8.2_400bps_hac@v4.3.0"

    # If run on FLO-PRO002 (R9.4.1) flow cell
    elif run.flow_cell_product_code == "FLO-PRO002":
        basecalling_model = MODELS_DIR / "dna_r9.4.1_e8_hac@v3.3"

    # Check if basecalling model was found
    if basecalling_model is None:
        raise ValueError(f"Could not find basecalling model for flow cell {run.flow_cell_product_code}")
    # Check if basecalling model exists
    if not basecalling_model.exists():
        raise ValueError(f"Basecalling model {basecalling_model} does not exist")

    # Get all modified base models based on base model
    for mod in MODIFICATIONS:
        mod_models = list(MODELS_DIR.glob(f"{basecalling_model.name}*{mod}*"))

        # If more than one model found, select the latest one and add it to the list
        if len(mod_models) > 0:
            latest_mod_model = get_latest_version(mod_models)
            modified_bases_models.append(latest_mod_model)

    return basecalling_model, modified_bases_models


# dorado="/faststorage/project/MomaReference/BACKUP/nanopore/software/dorado/dorado-0.5.1-linux-x64/bin/dorado"
# model="/faststorage/project/MomaReference/BACKUP/nanopore/models/dorado_models/dna_r10.4.1_e8.2_400bps_hac@v4.3.0"

# mod_base_model1="/faststorage/project/MomaReference/BACKUP/nanopore/models/dorado_models/dna_r10.4.1_e8.2_400bps_hac@v4.2.0_5mCG_5hmCG@v2"
# mod_base_model2="/faststorage/project/MomaReference/BACKUP/nanopore/models/dorado_models/dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2"
