import pytest
from typer.testing import CliRunner

from src.eldorado.main import app, find_pod5_dirs, get_pod5_dirs_for_basecalling


def test_help():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "pod5_files, bam_files, expected",
    [
        pytest.param(
            [],
            [],
            [],
            id="empty",
        ),
        pytest.param(
            ["sample/pod5/file.pod5"],
            [],
            ["sample/pod5"],
            id="single_sample_simple",
        ),
        pytest.param(
            ["sample/pod5/file.pod5"],
            ["sample/bam/file.bam"],
            [],
            id="single_sample_done_basecalling",
        ),
        pytest.param(
            ["sample_1/pod5/file.pod5", "sample_2/pod5/file.pod5"],
            ["sample_1/bam/file.bam"],
            ["sample_2/pod5"],
            id="multiple_samples_one_done_basecalling",
        ),
    ],
)
def test_filter_incomplete_pod5_dirs(tmp_path, pod5_files, bam_files, expected):
    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Add root dir to files
    pod5_files = [root_dir / x for x in pod5_files]
    bam_files = [root_dir / x for x in bam_files]

    # Create pod5 files
    for file in pod5_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Create bam files
    for file in bam_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Get unique pod5 dirs
    pod5_dirs = {x.parent for x in pod5_files}

    # Act
    result = get_pod5_dirs_for_basecalling(pod5_dirs)

    # Assert
    expected = [root_dir / x for x in expected]
    assert set(result) == set(expected)


@pytest.mark.parametrize(
    "pattern, pod5_files, expected",
    [
        pytest.param(
            "dir",
            [],
            [],
            id="empty",
        ),
        pytest.param(
            "sample",
            ["sample/file.pod5"],
            ["sample"],
            id="single_sample",
        ),
        pytest.param(
            "*/pod5",
            ["sample_1/pod5/file.pod5", "sample_2/pod5/file.pod5"],
            ["sample_1/pod5", "sample_2/pod5"],
            id="multiple_samples",
        ),
    ],
)
def test_get_pod5_dirs(tmp_path, pattern, pod5_files, expected):
    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Create files
    for file in pod5_files:
        file_path = root_dir / file
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.touch()

    # Act
    result = find_pod5_dirs(root_dir, pattern)

    # Assert
    expected = [root_dir / x for x in expected]
    assert set(result) == set(expected)
