import pytest
from typer.testing import CliRunner

from eldorado.main import app, find_pod5_dirs


def test_help():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0


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
    result = [x.path for x in result]
    assert set(result) == set(expected)
