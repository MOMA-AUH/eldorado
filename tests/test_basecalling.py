import pytest

from eldorado.basecalling import is_basecalling_complete, contains_pod5_files, is_version_newer, get_latest_version


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
