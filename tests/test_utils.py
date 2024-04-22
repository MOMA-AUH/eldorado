import pytest

from eldorado.utils import contains_pod5_files, get_pod5_dirs_from_pattern, needs_basecalling


@pytest.mark.parametrize(
    "pattern, dirs, expected",
    [
        pytest.param(
            "foo",
            [],
            [],
            id="empty",
        ),
        pytest.param(
            "foo",
            ["foo"],
            ["foo"],
            id="single_dir",
        ),
        pytest.param(
            "*/pod5",
            ["sample_1/pod5", "sample_2/pod5"],
            ["sample_1/pod5", "sample_2/pod5"],
            id="multiple_samples",
        ),
        pytest.param(
            "*/pod5",
            ["sample/other_dir"],
            [],
            id="dir_not_matching_pattern",
        ),
        pytest.param(
            "*/*/pod5",
            ["project/sample/pod5"],
            ["project/sample/pod5"],
            id="nested_dir",
        ),
    ],
)
def test_get_pod5_dirs_from_pattern(tmp_path, pattern, dirs, expected):
    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Create dirs
    for dd in dirs:
        dir_path = root_dir / dd
        dir_path.mkdir(parents=True, exist_ok=True)

    # Act
    result = get_pod5_dirs_from_pattern(root_dir, pattern)

    # Assert
    expected = [root_dir / x for x in expected]
    assert set(result) == set(expected)


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
    "pod5_files, other_files, expected",
    [
        pytest.param(
            ["pod5/file.pod5"],
            [],
            True,
            id="no_files",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["bam/file.bam"],
            False,
            id="existing_bam",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["fastq/file.fastq"],
            False,
            id="existing_fastq",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["fastq/file.fastq.gz"],
            False,
            id="existing_fastq_gz",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["fastq/file.txt", "sample/bam/file.txt"],
            True,
            id="existing_irrelevant_file",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["bam_suffix/file.bam"],
            False,
            id="suffix_in_bam_dir_name",
        ),
        pytest.param(
            ["pod5/file.pod5"],
            ["fastq_suffix/file.fastq"],
            False,
            id="suffix_in_fastq_dir_name",
        ),
    ],
)
def test_needs_basecalling(tmp_path, pod5_files, other_files, expected):
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
    result = needs_basecalling(pod5_dir)

    assert result == expected
