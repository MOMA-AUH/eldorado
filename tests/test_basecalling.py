import pytest

from eldorado.basecalling import cleanup_basecalling_lock_files, batch_should_be_skipped, BasecallingBatch
import eldorado.basecalling as basecalling
from eldorado.pod5_handling import BasecallingRun


@pytest.mark.usefixtures("mock_pod5_internals")
@pytest.mark.parametrize(
    "pod5_dir, existing_files, min_batch_size, excepted",
    [
        pytest.param(
            "pod5",
            [],
            1,
            True,
            id="Empty pod5 dir",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
            ],
            1,
            False,
            id="Single pod5 file needs to be basecalled",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file_1.pod5",
                "pod5/file_2.pod5",
            ],
            1,
            False,
            id="Two pod5 files need to be basecalled",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            1,
            True,
            id="Skip pod5 file with lock file",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/done_files/file.pod5.done",
            ],
            1,
            True,
            id="Skip pod5 file with done file",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "pod5/new_file.pod5",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            1,
            False,
            id="One pod5 locked and one new pod5 file for basecalling",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/new_file.pod5",
            ],
            2,
            True,
            id="Single new pod5 file with min batch size 2",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "pod5/new_file.pod5",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            2,
            True,
            id="One pod5 locked and one new pod5 file with min batch size 2",
        ),
    ],
)
def test_batch_should_be_skipped(tmp_path, pod5_dir, existing_files, min_batch_size, excepted):
    # Arrange

    # Insert root dir
    pod5_dir = tmp_path / pod5_dir
    existing_files = [tmp_path / file for file in existing_files]

    # Create existing files
    for file in existing_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    run = BasecallingRun(pod5_dir)
    batch = BasecallingBatch(run)
    result = batch_should_be_skipped(
        basecalling_batch=batch,
        min_batch_size=min_batch_size,
    )

    # Assert
    assert result == excepted


@pytest.mark.usefixtures("mock_pod5_internals")
@pytest.mark.parametrize(
    "pod5_dir, existing_files, expected_generated_files",
    [
        pytest.param(
            "pod5",
            [],
            [],
            id="Empty",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
            ],
            [
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            id="Single pod5 file needs to be basecalled",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file_1.pod5",
                "pod5/file_2.pod5",
            ],
            [
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/lock_files/file_1.pod5.lock",
                "bam_eldorado/basecalling/lock_files/file_2.pod5.lock",
            ],
            id="Two pod5 files need to be basecalled",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            [],
            id="Skip pod5 file with lock file",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/done_files/file.pod5.done",
            ],
            [],
            id="Skip pod5 file with done file",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "pod5/new_file.pod5",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            [
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/lock_files/new_file.pod5.lock",
            ],
            id="One pod5 done and one new pod5 file for basecalling",
        ),
    ],
)
def test_basecalling_batch_write_files(monkeypatch, tmp_path, pod5_dir, existing_files, expected_generated_files):
    # Mock time
    monkeypatch.setattr(basecalling.time, "time", lambda: "1234")

    # Mock submit to slurm
    def mock_submit_basecalling_batch_to_slurm(*args, **kwargs):
        return "JOBID"

    monkeypatch.setattr(basecalling, "submit_basecalling_batch_to_slurm", mock_submit_basecalling_batch_to_slurm)

    # Arrange
    # Insert root dir
    pod5_dir = tmp_path / pod5_dir
    existing_files = [tmp_path / file for file in existing_files]
    expected_generated_files = [tmp_path / file for file in expected_generated_files]

    # Create existing files
    for file in existing_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    run = BasecallingRun(pod5_dir)
    batch = BasecallingBatch(run)
    batch.write_files()

    # Assert
    all_files = {x for x in tmp_path.rglob("*") if x.is_file()}
    new_files = all_files - set(existing_files)

    assert new_files == set(expected_generated_files)


@pytest.mark.usefixtures("mock_pod5_internals")
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
                "bam_eldorado/basecalling/batches/1234/slurm_id.txt",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            True,
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/slurm_id.txt",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            id="Pod5 file in queue",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/slurm_id.txt",
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            False,
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/slurm_id.txt",
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
        pod5_dir=BasecallingRun(pod5_dir),
    )

    # Assert
    all_files = {x for x in tmp_path.rglob("*") if x.is_file()}

    assert all_files == set(expected_files_after_cleanup)
