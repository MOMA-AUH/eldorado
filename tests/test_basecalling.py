import pytest

import eldorado.basecalling as basecalling
from eldorado.basecalling import (
    BasecallingBatch,
    cleanup_basecalling_lock_files,
    file_size,
    split_files_into_groups,
)
from eldorado.pod5_handling import SequencingRun
from tests.conftest import create_files


@pytest.mark.parametrize(
    "pod5_files_count",
    [
        pytest.param(0, id="Edge case with no pod5 files"),
        pytest.param(1, id="Happy path with 1 pod5 files"),
        pytest.param(3, id="Happy path with 3 pod5 files"),
    ],
)
def test_setup(pod5_files_count, tmp_path):
    # Arrange
    pod5_dir = tmp_path / "pod5"
    pod5_files = [pod5_dir / f"file{i}.pod5" for i in range(pod5_files_count)]
    create_files(pod5_files)

    run = SequencingRun(pod5_dir)
    batch = BasecallingBatch(run=run, pod5_files=pod5_files)

    # Act
    batch.setup()

    # Assert
    assert batch.working_dir.exists()
    assert all(lock_file.parent.exists() for lock_file in batch.pod5_lock_files)
    assert batch.pod5_manifest.read_text(encoding="utf-8") == "\n".join(str(file) for file in pod5_files) + "\n"


@pytest.mark.parametrize(
    "pod5_dir, existing_files, expected",
    [
        pytest.param(
            "pod5",
            [],
            0,
            id="Empty pod5 dir",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
            ],
            1,
            id="Single file",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file_1.pod5",
                "pod5/file_2.pod5",
            ],
            2,
            id="Two files",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file_1.pod5",
                "pod5/file_2.pod5",
                "pod5/file_3.pod5",
            ],
            3,
            id="Three files",
        ),
    ],
)
def test_file_size(tmp_path, pod5_dir, existing_files, expected):
    # Arrange

    # Insert root dir
    pod5_dir = tmp_path / pod5_dir
    existing_files = [tmp_path / file for file in existing_files]

    # Create existing files
    for file in existing_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()
        # Write 1 B to the file
        file.write_text("a")

    # Act
    result = file_size(files=existing_files)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "pod5_dir, existing_files, pod5_is_in_queue, expected_files_after_cleanup",
    [
        pytest.param(
            "pod5",
            [],
            False,
            [],
            id="Empty",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/batch_job_id.txt",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            True,
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/batch_job_id.txt",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            id="Pod5 file in queue",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/batch_job_id.txt",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            False,
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/batch_job_id.txt",
            ],
            id="Missing pod5 manifest",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            False,
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
            ],
            id="Missing slurm id file",
        ),
    ],
)
def test_cleanup_stalled_batch_basecalling_dirs_and_lock_files(
    monkeypatch,
    tmp_path,
    pod5_dir,
    existing_files,
    pod5_is_in_queue,
    expected_files_after_cleanup,
):
    # Mock is_file_inactive
    def mock_is_in_queue(*args, **kwargs):
        return pod5_is_in_queue

    monkeypatch.setattr(basecalling, "is_in_queue", mock_is_in_queue)

    def mock_read_pod5_manifest(*args, **kwargs):
        return [tmp_path / "pod5" / "file.pod5"]

    monkeypatch.setattr(basecalling, "read_pod5_manifest", mock_read_pod5_manifest)

    # Arrange
    # Insert tmp_path
    pod5_dir = tmp_path / pod5_dir
    existing_files = [tmp_path / file for file in existing_files]
    expected_files_after_cleanup = [tmp_path / file for file in expected_files_after_cleanup]

    # Create existing files
    for file in existing_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    cleanup_basecalling_lock_files(
        pod5_dir=SequencingRun(pod5_dir),
    )

    # Assert
    all_files = {x for x in tmp_path.rglob("*") if x.is_file()}

    assert all_files == set(expected_files_after_cleanup)


@pytest.mark.parametrize(
    "max_batch_size, unbasecalled_pod5_files, expected_groups",
    [
        pytest.param(
            1,
            [],
            [],
            id="Empty",
        ),
        pytest.param(
            1,
            [
                "file_1.pod5",
                "file_2.pod5",
                "file_3.pod5",
            ],
            [
                ["file_1.pod5"],
                ["file_2.pod5"],
                ["file_3.pod5"],
            ],
            id="Three files with max batch size of 1",
        ),
        pytest.param(
            2,
            [
                "file_1.pod5",
                "file_2.pod5",
                "file_3.pod5",
            ],
            [
                ["file_1.pod5", "file_2.pod5"],
                ["file_3.pod5"],
            ],
            id="Three files with max batch size of 2",
        ),
        pytest.param(
            3,
            [
                "file_1.pod5",
                "file_2.pod5",
                "file_3.pod5",
            ],
            [
                ["file_1.pod5", "file_2.pod5", "file_3.pod5"],
            ],
            id="Three files with max batch size of 3",
        ),
        pytest.param(
            4,
            [
                "file_1.pod5",
                "file_2.pod5",
                "file_3.pod5",
            ],
            [
                ["file_1.pod5", "file_2.pod5", "file_3.pod5"],
            ],
            id="Three files with max batch size of 4",
        ),
    ],
)
def test_split_files_into_groups(tmp_path, max_batch_size, unbasecalled_pod5_files, expected_groups):
    # Arrange
    # Insert tmp_path
    unbasecalled_pod5_files = [tmp_path / file for file in unbasecalled_pod5_files]

    # Create existing files
    for file in unbasecalled_pod5_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()
        # Write 1B to the file
        file.write_text("a")

    # Act
    groups = split_files_into_groups(max_batch_size, unbasecalled_pod5_files)

    # Assert
    observed_groups = [[file.name for file in group] for group in groups]
    assert observed_groups == expected_groups
