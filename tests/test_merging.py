import pytest

from eldorado.merging import all_pod5_files_are_basecalled, get_done_batch_dirs
from eldorado.pod5_handling import SequencingRun


@pytest.mark.usefixtures("mock_pod5_internals")
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
    pod5_dir,
    files,
    expected,
):
    # Arrange

    # Insert tmp directory in path
    pod5_dir = tmp_path / pod5_dir
    pod5_dir.mkdir(parents=True, exist_ok=True)
    files = [tmp_path / file for file in files]

    # Create files
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    pod5_dir = SequencingRun(pod5_dir)
    result = all_pod5_files_are_basecalled(pod5_dir=pod5_dir)

    # Assert
    assert result == expected


@pytest.mark.usefixtures("mock_pod5_internals")
@pytest.mark.parametrize(
    "pod5_dir, files, expected",
    [
        pytest.param(
            "",
            [],
            [],
            id="Empty",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/basecalling/batches/1/batch.done",
                "sample/bam_eldorado/basecalling/batches/1/file.bam",
            ],
            [
                "sample/bam_eldorado/basecalling/batches/1",
            ],
            id="Single batch done",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/basecalling/batches/1/batch.done",
                "sample/bam_eldorado/basecalling/batches/1/file.bam",
                "sample/bam_eldorado/basecalling/batches/2/batch.done",
                "sample/bam_eldorado/basecalling/batches/2/file.bam",
                "sample/bam_eldorado/basecalling/batches/3/batch.done",
                "sample/bam_eldorado/basecalling/batches/3/file.bam",
            ],
            [
                "sample/bam_eldorado/basecalling/batches/1",
                "sample/bam_eldorado/basecalling/batches/2",
                "sample/bam_eldorado/basecalling/batches/3",
            ],
            id="Multiple batches done",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/basecalling/batches/1/batch.done",
                "sample/bam_eldorado/basecalling/batches/1/file.bam",
                "sample/bam_eldorado/basecalling/batches/2/other.file",
                "sample/bam_eldorado/basecalling/batches/2/file.bam",
            ],
            [
                "sample/bam_eldorado/basecalling/batches/1",
            ],
            id="Multiple batches not all done",
        ),
    ],
)
def test_get_done_batch_dirs(
    tmp_path,
    pod5_dir,
    files,
    expected,
):
    # Arrange
    # Insert root directory
    pod5_dir = tmp_path / pod5_dir
    files = [tmp_path / file for file in files]
    expected = [tmp_path / file for file in expected]

    # Create files
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    result = get_done_batch_dirs(SequencingRun(pod5_dir))

    # Assert
    assert result == expected
