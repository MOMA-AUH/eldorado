from pathlib import Path
from typing import List

import pytest

import eldorado.configuration as configuration
from eldorado.configuration import (
    DoradoConfig,
    ProjectConfig,
    get_latest_version,
    get_modification_models,
    get_project_configs,
    is_basecalling_model_path_valid,
    is_dorado_executable_valid,
    is_row_inputs_valid,
    is_version_newer,
    unpack_config_row,
)
from eldorado.constants import ACCOUNT, BASECALLING_MODEL, DORADO_EXECUTABLE, MOD_5MCG_5HMCG, MOD_6MA, PROJECT_ID


# Helper functions
def create_files(files):
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()


def test_config_save_and_load(tmp_path):
    # Arrange
    config = DoradoConfig(
        dorado_executable=Path("/path/to/dorado"),
        basecalling_model=Path("/path/to/basecalling_model"),
        modification_models=[Path("/path/to/modification_model")],
    )
    config_path = Path(tmp_path / "config.json")

    # Act
    config.save(config_path)
    loaded_config = DoradoConfig.load(config_path)

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

    create_files(models)

    # Act
    result = get_latest_version(models)

    # Assert
    assert result.name == expected


@pytest.mark.parametrize(
    "files_in_model_dir, model, mod_5mcg_5hmcg,mod_6ma, expected",
    [
        pytest.param(
            [],
            "",
            False,
            False,
            [],
            id="empty",
        ),
        pytest.param(
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            ],
            "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            False,
            False,
            [],
            id="no_mods",
        ),
        pytest.param(
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_5mCG_5hmCG@v1",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2",
            ],
            "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            False,
            True,
            ["dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2"],
            id="6mA only",
        ),
        pytest.param(
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_5mCG_5hmCG@v1",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2",
            ],
            "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            True,
            False,
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_5mCG_5hmCG@v1",
            ],
            id="5mc_5hmCG only",
        ),
        pytest.param(
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_5mCG_5hmCG@v1",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2",
            ],
            "dna_r10.4.1_e8.2_400bps_hac@v4.3.0",
            True,
            True,
            [
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_5mCG_5hmCG@v1",
                "dna_r10.4.1_e8.2_400bps_hac@v4.3.0_6mA@v2",
            ],
            id="Both mods",
        ),
    ],
)
def test_get_modification_models(
    tmp_path,
    files_in_model_dir: List[str],
    model: str,
    mod_5mcg_5hmcg: bool,
    mod_6ma: bool,
    expected: List[str],
):
    # Arrange
    model_path = tmp_path / model
    models = [tmp_path / x for x in files_in_model_dir]
    expected = [tmp_path / x for x in expected]

    # Create model files
    create_files(models)

    # Act
    result = get_modification_models(
        basecalling_model=model_path,
        mod_5mcg_5hmcg=mod_5mcg_5hmcg,
        mod_6ma=mod_6ma,
    )

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "project_id, account, dorado_executable, basecalling_model, mod_5mcg_5hmcg, mod_6ma",
    [
        pytest.param(
            "N999",
            "",
            "",
            "",
            "",
            "",
            id="Empty",
        ),
        pytest.param(
            "N123",
            "my_account",
            "path/to/dorado",
            "v1.0.0",
            "1",
            "1",
            id="Filled out",
        ),
    ],
)
def test_unpack_project_config(
    project_id: str,
    account: str,
    dorado_executable: str,
    basecalling_model: str,
    mod_5mcg_5hmcg: str,
    mod_6ma: str,
):
    # Arrange
    row = {
        PROJECT_ID: project_id,
        ACCOUNT: account,
        DORADO_EXECUTABLE: dorado_executable,
        BASECALLING_MODEL: basecalling_model,
        MOD_5MCG_5HMCG: mod_5mcg_5hmcg,
        MOD_6MA: mod_6ma,
    }

    # Act
    project_id_res, account_res, dorado_executable_res, basecalling_model_res, mod_5mcg_5hmcg_res, mod_6ma_res = unpack_config_row(row)

    # Assert
    assert project_id_res == project_id
    assert account_res == account
    assert dorado_executable_res == dorado_executable
    assert basecalling_model_res == basecalling_model
    assert mod_5mcg_5hmcg_res == mod_5mcg_5hmcg
    assert mod_6ma_res == mod_6ma


@pytest.mark.parametrize(
    "csv_body, expected",
    [
        pytest.param(
            "",
            [],
            id="Empty",
        ),
        pytest.param(
            """\
                project_id,account,dorado_executable,basecalling_model,mod_5mcg_5hmcg,mod_6ma
            """,
            [],
            id="Header only",
        ),
        pytest.param(
            """\
                project_id,account,dorado_executable,basecalling_model,mod_5mcg_5hmcg,mod_6ma
                default,path/to/dorado,path/to/model,1,1
            """,
            [],
            id="Default only",
        ),
        pytest.param(
            """\
                project_id,account,dorado_executable,basecalling_model,mod_5mcg_5hmcg,mod_6ma
                default,my_account,path/to/dorado,path/to/model,1,1
                project1,my_account,path/to/dorado,path/to/model,1,1
            """,
            [
                ProjectConfig(
                    "project1",
                    "my_account",
                    Path("path/to/dorado"),
                    Path("path/to/model"),
                    True,
                    True,
                )
            ],
            id="One valid project - All filled out",
        ),
        pytest.param(
            """\
                project_id,account,dorado_executable,basecalling_model,mod_5mcg_5hmcg,mod_6ma
                default,my_account,path/to/dorado,path/to/model,1,1
                project1,,,,
            """,
            [
                ProjectConfig(
                    "project1",
                    "my_account",
                    Path("path/to/dorado"),
                    Path("path/to/model"),
                    True,
                    True,
                )
            ],
            id="One valid project - Filled out from default",
        ),
        pytest.param(
            """\
                project_id,account,dorado_executable,basecalling_model,mod_5mcg_5hmcg,mod_6ma
                default,my_account,path/to/dorado,auto,1,1
                project1,,,,,
            """,
            [
                ProjectConfig(
                    "project1",
                    "my_account",
                    Path("path/to/dorado"),
                    None,
                    True,
                    True,
                )
            ],
            id="One valid project - Default: Use auto",
        ),
        pytest.param(
            """\
                project_id,account,dorado_executable,basecalling_model,mod_5mcg_5hmcg,mod_6ma
                default,my_account,path/to/dorado,path/to/model,1,1
                project1,,,auto,,
            """,
            [
                ProjectConfig(
                    "project1",
                    "my_account",
                    Path("path/to/dorado"),
                    None,
                    True,
                    True,
                )
            ],
            id="One valid project - Default: Use newest model",
        ),
    ],
)
def test_project_config_is_invalid(monkeypatch, tmp_path: Path, csv_body: str, expected: bool):
    # Arrange
    csv_file = tmp_path / "config.csv"
    csv_file.write_text(csv_body)

    # Mock check for dorado executable and basecalling model
    monkeypatch.setattr(configuration, "is_basecalling_model_path_valid", lambda x: True)
    monkeypatch.setattr(configuration, "is_dorado_executable_valid", lambda x: True)

    # Act
    result = get_project_configs(csv_file)

    # Assert
    assert result == expected


class TestIsBasecallingModelPathValid:
    def test_valid_path(self, tmp_path):
        # Arrange
        model_path = tmp_path / "model"
        model_path.mkdir()

        # Act
        result = is_basecalling_model_path_valid(model_path)

        # Assert
        assert result is True

    def test_path_is_file(self, tmp_path):
        # Arrange
        model_path = tmp_path / "model"
        model_path.touch()

        # Act
        result = is_basecalling_model_path_valid(model_path)

        # Assert
        assert result is False

    def test_path_does_not_exist(self, tmp_path):
        # Arrange
        model_path = tmp_path / "model"

        # Act
        result = is_basecalling_model_path_valid(model_path)

        # Assert
        assert result is False


class TestIsDoradoExecutableValid:
    @pytest.mark.parametrize(
        "dorado_executable_name, expected",
        [
            pytest.param("dorado", True, id="Valid name"),
            pytest.param("dorado.exe", False, id="Wrong name"),
        ],
    )
    def test_name(self, tmp_path, dorado_executable_name, expected):
        # Arrange
        model_path = tmp_path / dorado_executable_name
        model_path.touch()

        # Act
        result = is_dorado_executable_valid(model_path)

        # Assert
        assert result is expected

    def test_path_is_dir(self, tmp_path):
        # Arrange
        model_path = tmp_path / "dorado"
        model_path.mkdir()

        # Act
        result = is_dorado_executable_valid(model_path)

        # Assert
        assert result is False

    def test_path_does_not_exist(self, tmp_path):
        # Arrange
        model_path = tmp_path / "dorado"

        # Act
        result = is_dorado_executable_valid(model_path)

        # Assert
        assert result is False


@pytest.fixture
def project_defaults():
    return ProjectConfig(
        project_id="default_project",
        account="default_account",
        dorado_executable=Path("/default/path/to/dorado"),
        basecalling_model=Path("/default/path/to/model"),
        mod_5mcg_5hmcg=True,
        mod_6ma=False,
    )


@pytest.mark.parametrize(
    "row, expected",
    [
        pytest.param(
            {
                "project_id": "1",
                "account": "",
                "dorado_executable": "",
                "basecalling_model": "",
                "mod_5mcg_5hmcg": "",
                "mod_6ma": "",
            },
            True,
            id="Empty values",
        ),
        pytest.param(
            {
                "project_id": "2",
                "account": "my_account",
                "dorado_executable": "/path/to/dorado",
                "basecalling_model": "/path/to/model",
                "mod_5mcg_5hmcg": "1",
                "mod_6ma": "0",
            },
            True,
            id="Simple case",
        ),
        pytest.param(
            {
                "project_id": "3",
                "account": "my_account",
                "dorado_executable": "",
                "basecalling_model": "",
                "mod_5mcg_5hmcg": "not_bool",
                "mod_6ma": "",
            },
            False,
            id="Invalid bool values - String",
        ),
    ],
)
def test_is_row_inputs_valid(monkeypatch, row, expected):
    # Arrange
    # Mock check for dorado executable and basecalling model
    monkeypatch.setattr(configuration, "is_basecalling_model_path_valid", lambda x: True)
    monkeypatch.setattr(configuration, "is_dorado_executable_valid", lambda x: True)

    # Act
    result = is_row_inputs_valid(row)

    # Assert
    assert result == expected
