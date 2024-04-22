import pytest

from eldorado.merging import all_existing_pod5_files_basecalled, all_existing_batches_are_done
import eldorado.my_dataclasses as my_dataclasses
from eldorado.my_dataclasses import Pod5Directory


@pytest.mark.parametrize(
    "pod5_dir, files, expected",
    [
        pytest.param(
            "sample/pod5",
            [],
            True,
            id="Empty",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/pod5/file.pod5",
                "sample/bam_eldorado/basecalling/done_files/file.pod5.done",
            ],
            True,
            id="Single file",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/pod5/file1.pod5",
                "sample/pod5/file2.pod5",
                "sample/bam_eldorado/basecalling/done_files/file1.pod5.done",
                "sample/bam_eldorado/basecalling/done_files/file2.pod5.done",
            ],
            True,
            id="Multiple files",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/pod5/file.pod5",
                "sample/pod5/file2.pod5",
                "sample/bam_eldorado/basecalling/done_files/file.pod5.done",
            ],
            False,
            id="Missing done file",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/pod5/file.pod5",
                "sample/bam_eldorado/basecalling/done_files/other_file.pod5.done",
            ],
            False,
            id="Wrong done file",
        ),
    ],
)
def test_all_existing_pod5_files_basecalled(
    tmp_path,
    monkeypatch,
    pod5_dir,
    files,
    expected,
):
    # Arrange
    # Mock is_file_inactive
    def mock_is_file_inactive(*args, **kwargs):
        return True

    monkeypatch.setattr(my_dataclasses, "is_file_inactive", mock_is_file_inactive)

    # Insert tmp directory in path
    pod5_dir = tmp_path / pod5_dir
    pod5_dir.mkdir(parents=True, exist_ok=True)
    files = [tmp_path / file for file in files]

    # Create files
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    pod5_dir = Pod5Directory(pod5_dir)
    result = all_existing_pod5_files_basecalled(pod5_dir=pod5_dir)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "pod5_dir, files, expected",
    [
        pytest.param(
            "",
            [],
            True,
            id="Empty",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/basecalling/batches/1/batch.done",
            ],
            True,
            id="Single batch done",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/basecalling/batches/1/batch.done",
                "sample/bam_eldorado/basecalling/batches/2/batch.done",
            ],
            True,
            id="Multiple batches done",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/basecalling/batches/1/batch.done",
                "sample/bam_eldorado/basecalling/batches/2/other.file",
            ],
            False,
            id="Multiple batches not all done",
        ),
    ],
)
def test_all_existing_batches_are_done(tmp_path, pod5_dir, files, expected):
    # Arrange
    # Insert root directory
    pod5_dir = tmp_path / pod5_dir
    files = [tmp_path / file for file in files]

    # Create files
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    result = all_existing_batches_are_done(Pod5Directory(pod5_dir))

    # Assert
    assert result == expected
