from typing import List

from pathlib import Path
import pytest

import eldorado
from eldorado.basecalling import (
    is_version_newer,
    get_latest_version,
    get_modified_bases_models,
    process_unbasecalled_pod5_files,
    cleanup_stalled_batch_basecalling_dirs_and_lock_files,
)
from eldorado.my_dataclasses import Pod5Directory
import eldorado.utils


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


@pytest.mark.parametrize(
    "pod5_dir, existing_files, min_batch_size, expected_generated_files",
    [
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
            ],
            1,
            [
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/run_basecaller_batch_1234.sh",
                "bam_eldorado/basecalling/batches/1234/slurm_id.txt",
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
            1,
            [
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/run_basecaller_batch_1234.sh",
                "bam_eldorado/basecalling/batches/1234/slurm_id.txt",
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
            1,
            [],
            id="Skip pod5 file with lock file",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/basecalling/done_files/file.pod5.done",
            ],
            1,
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
            1,
            [
                "bam_eldorado/basecalling/batches/1234/pod5_manifest.txt",
                "bam_eldorado/basecalling/batches/1234/run_basecaller_batch_1234.sh",
                "bam_eldorado/basecalling/batches/1234/slurm_id.txt",
                "bam_eldorado/basecalling/lock_files/new_file.pod5.lock",
            ],
            id="One pod5 locked and one new pod5 file",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/new_file.pod5",
            ],
            2,
            [],
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
            [],
            id="One pod5 locked and one new pod5 file with min batch size 2",
        ),
    ],
)
def test_process_unbasecalled_pod5_dirs(monkeypatch, tmp_path, pod5_dir, existing_files, min_batch_size, expected_generated_files):
    # Mock time
    monkeypatch.setattr(eldorado.my_dataclasses.time, "time", lambda: 1234)

    # Mock is_file_inactive
    def mock_is_file_inactive(*args, **kwargs):
        return True

    monkeypatch.setattr(eldorado.my_dataclasses, "is_file_inactive", mock_is_file_inactive)

    # Mock get model
    def mock_get_basecalling_model(*args, **kwargs):
        return Path("path/to/model")

    monkeypatch.setattr(eldorado.basecalling, "get_basecalling_model", mock_get_basecalling_model)

    class MockCompletedProcess:
        stdout = b"12345678"

    def mock_subprocess_run(*args, **kwargs):
        return MockCompletedProcess()

    monkeypatch.setattr(eldorado.basecalling.subprocess, "run", mock_subprocess_run)

    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Insert root dir
    existing_files = [root_dir / file for file in existing_files]
    expected_generated_files = [root_dir / file for file in expected_generated_files]

    # Create existing files
    for file in existing_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    pod5_dir = root_dir / pod5_dir
    pod5_dir = Pod5Directory(pod5_dir)
    process_unbasecalled_pod5_files(
        pod5_dir=pod5_dir,
        modifications=[],
        min_batch_size=min_batch_size,
        dry_run=False,
    )

    # Assert
    all_files = {x for x in root_dir.rglob("*") if x.is_file()}
    new_files = all_files - set(existing_files)

    assert new_files == set(expected_generated_files)


@pytest.mark.parametrize(
    "pod5_dir, existing_files, is_in_queue, expected_files_after_cleanup",
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
                "bam_eldorado/basecalling/lock_files/file.pod5.lock",
            ],
            False,
            [
                "pod5/file.pod5",
            ],
            id="Pod5 file not in queue",
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
            ],
            id="Pod5 file in queue",
        ),
    ],
)
def test_cleanup_stalled_batch_basecalling_dirs_and_lock_files(
    monkeypatch,
    tmp_path,
    pod5_dir,
    existing_files,
    is_in_queue,
    expected_files_after_cleanup,
):
    # Mock is_file_inactive
    def mock_is_in_queue(*args, **kwargs):
        return is_in_queue

    monkeypatch.setattr(eldorado.utils, "is_in_queue", mock_is_in_queue)

    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Insert root dir
    pod5_dir = root_dir / pod5_dir
    existing_files = [root_dir / file for file in existing_files]
    expected_files_after_cleanup = [root_dir / file for file in expected_files_after_cleanup]

    # Create existing files
    for file in existing_files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    cleanup_stalled_batch_basecalling_dirs_and_lock_files(
        pod5_dir=Pod5Directory(pod5_dir),
    )

    # Assert
    all_files = {x for x in root_dir.rglob("*") if x.is_file()}

    assert all_files == set(expected_files_after_cleanup)
