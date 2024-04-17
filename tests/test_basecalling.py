from typing import List

import pytest

from eldorado.basecalling import is_basecalling_complete, contains_pod5_files, is_version_newer, get_latest_version, get_modified_bases_models


@pytest.mark.parametrize(
    "pod5_files, other_files, expected",
    [
        pytest.param(
            ["pod5/file.pod5"],
            [],
            False,
            id="no_files",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["bam/file.bam"],
            True,
            id="existing_bam",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["fastq/file.fastq"],
            True,
            id="existing_fastq",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["fastq/file.fastq.gz"],
            True,
            id="existing_fastq_gz",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["fastq/file.txt", "sample/bam/file.txt"],
            False,
            id="existing_irrelevant_file",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["bam_suffix/file.bam"],
            True,
            id="suffix_in_bam_dir_name",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["fastq_suffix/file.fastq"],
            True,
            id="suffix_in_fastq_dir_name",
        ),
    ],
)
def test_get_pod5_dirs_for_basecalling(tmp_path, pod5_files, other_files, expected):
    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Add root dir to files
    pod5_files = [root_dir / x for x in pod5_files]
    other_files = [root_dir / x for x in other_files]

    # Create pod5 files
    for file in pod5_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Create bam files
    for file in other_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    pod5_dir = pod5_files[0].parent
    result = is_basecalling_complete(pod5_dir)

    assert result == expected


@pytest.mark.parametrize(
    "pod5_files, expected",
    [
        pytest.param(
            [],
            False,
            id="empty",
        ),
        pytest.param(
            ["file.pod5"],
            True,
            id="single_file",
        ),
        pytest.param(
            ["file.pod5", "file2.pod5"],
            True,
            id="multiple_files",
        ),
        pytest.param(
            ["file.txt"],
            False,
            id="wrong_extension",
        ),
    ],
)
def test_contains_pod5_files(tmp_path, pod5_files, expected):
    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Add root dir to files
    pod5_files = [root_dir / x for x in pod5_files]

    # Create pod5 files
    for file in pod5_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    result = contains_pod5_files(root_dir)

    # Assert
    assert result == expected


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
    result = get_modified_bases_models(model, model_dir_path, mods)

    # Assert
    assert result == expected
