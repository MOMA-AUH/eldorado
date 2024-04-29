from typing import List

from pathlib import Path

import pytest

from eldorado.configuration import is_version_newer, get_latest_version, get_modified_bases_models, Config


def test_config_save_and_load(tmp_path):
    # Arrange
    config = Config(
        dorado_executable=Path("/path/to/dorado"),
        basecalling_model=Path("/path/to/basecalling_model"),
        modification_models=[Path("/path/to/modification_model")],
    )
    config_path = Path(tmp_path / "config.json")

    # Act
    config.save(config_path)
    loaded_config = Config.load(config_path)

    # Assert
    assert config == loaded_config
    assert config_path.exists()


@pytest.mark.parametrize(
    "current_version, candidate_version, expected",
    [
        pytest.param(
            "1.0.0",
            "1.0.0",
            False,
            id="equal",
        ),
        pytest.param(
            "1.0.0",
            "1.0.1",
            True,
            id="minor_version",
        ),
        pytest.param(
            "1.0.0",
            "1.1.0",
            True,
            id="major_version",
        ),
        pytest.param(
            "1.0.0",
            "2.0.0",
            True,
            id="major_version",
        ),
    ],
)
def test_compare_versions(current_version, candidate_version, expected):
    # Act
    result = is_version_newer(current_version, candidate_version)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "models, expected",
    [
        pytest.param(
            ["model@v1.0.0"],
            "model@v1.0.0",
            id="single_model",
        ),
        pytest.param(
            ["model@v1.0.0", "model@v1.0.1", "model@v1.1.0"],
            "model@v1.1.0",
            id="multiple_models",
        ),
        pytest.param(
            ["model@v1.0", "model@v1.0.0.3", "model@v2"],
            "model@v2",
            id="multiple_models_variants",
        ),
    ],
)
def test_get_latest_version(tmp_path, models, expected):
    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Add root dir to files
    models = [root_dir / x for x in models]

    # Create model files
    for file in models:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    result = get_latest_version(models)

    # Assert
    assert result.name == expected


@pytest.mark.parametrize(
    "files_in_model_dir, model, mods, expected",
    [
        pytest.param(
            [],
            "",
            [],
            [],
            id="empty",
        ),
        pytest.param(
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            ],
            "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            [],
            [],
            id="no_mods",
        ),
        pytest.param(
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2",
            ],
            "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            ["6mA"],
            ["dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2"],
            id="single_model",
        ),
        pytest.param(
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_5mCG_5hmCG@v1",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2",
            ],
            "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            ["5mCG_5hmCG", "6mA"],
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_5mCG_5hmCG@v1",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2",
            ],
            id="multiple_models",
        ),
    ],
)
def test_get_modified_bases_models(tmp_path, files_in_model_dir: List[str], model: str, mods: List[str], expected: List[str]):
    # Arrange
    model_dir_path = tmp_path / "models"
    model_dir_path.mkdir()

    # Add root dir to files
    models = [model_dir_path / x for x in files_in_model_dir]
    expected = [model_dir_path / x for x in expected]

    # Create model files
    for file in models:
        file.touch()

    # Act
    result = get_modified_bases_models(
        basecalling_model=model,
        modifications=mods,
        models_dir=model_dir_path,
    )

    # Assert
    assert result == expected
