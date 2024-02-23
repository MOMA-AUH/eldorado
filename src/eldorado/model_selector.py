import re
from pathlib import Path
from typing import List, Tuple


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
    for i in range(len(v1_components)):
        if v1_components[i] >= v2_components[i]:
            return True
        elif v1_components[i] < v2_components[i]:
            return False

    return False


def extract_version(path: Path) -> str:
    pattern = r"@v([\.\d+]{0,3})$"
    return re.findall(pattern, path.name)[0]


def find_latest_version(paths: List[Path]):
    latest_version = None
    latest_path = None

    for path in paths:
        version = extract_version(path)
        if version:
            if latest_version is None or compare_versions(version, latest_version):
                latest_version = version
                latest_path = path

    return latest_path


def model_selector() -> Tuple[Path, List[Path]]:
    basecalling_model = Path("/faststorage/project/MomaReference/BACKUP/nanopore/models/dorado_models/dna_r10.4.1_e8.2_400bps_hac@v4.3.0")

    # Get all modified base models based on base model
    methylation_models = list(basecalling_model.parent.glob(basecalling_model.name + "*5mCG_5hmCG*"))
    adination_models = list(basecalling_model.parent.glob(basecalling_model.name + "*6mA*"))

    # Select the model with the highest version
    modified_bases_models = []
    if len(methylation_models) > 0:
        modified_bases_models.append(find_latest_version(methylation_models))

    if len(adination_models) > 0:
        modified_bases_models.append(find_latest_version(adination_models))

    return basecalling_model, modified_bases_models


# dorado="/faststorage/project/MomaReference/BACKUP/nanopore/software/dorado/dorado-0.5.1-linux-x64/bin/dorado"
# model="/faststorage/project/MomaReference/BACKUP/nanopore/models/dorado_models/dna_r10.4.1_e8.2_400bps_hac@v4.3.0"

# mod_base_model1="/faststorage/project/MomaReference/BACKUP/nanopore/models/dorado_models/dna_r10.4.1_e8.2_400bps_hac@v4.2.0_5mCG_5hmCG@v2"
# mod_base_model2="/faststorage/project/MomaReference/BACKUP/nanopore/models/dorado_models/dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2"
