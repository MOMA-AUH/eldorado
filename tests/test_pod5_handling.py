from pathlib import Path
from unittest.mock import MagicMock

from typing import List

import pytest

from eldorado import pod5_handling
from eldorado.pod5_handling import SequencingRun, is_file_inactive, needs_basecalling, contains_pod5_files, get_pod5_dirs_from_pattern
from tests.test_utils import create_files


@pytest.mark.parametrize(
    "time_created, time_now, min_time, expected",
    [
        pytest.param(0, 6, 5, True, id="done"),
        pytest.param(0, 4, 5, False, id="not_done"),
    ],
)
def test_is_file_inactive(monkeypatch, time_created, time_now, min_time, expected):
    # Arrange
    monkeypatch.setattr(pod5_handling.time, "time", lambda *args, **kwargs: time_now)
    monkeypatch.setattr(pod5_handling.Path, "stat", lambda *args, **kwargs: MagicMock(st_mtime=time_created))

    # Act
    result = is_file_inactive(Path("file"), min_time)

    # Assert
    assert result == expected


@pytest.mark.usefixtures("mock_pod5_internals")
@pytest.mark.parametrize(
    "pod5_dir, pod5_files, final_summary, final_summary_text, expected",
    [
        pytest.param(
            "sample/pod5",
            [],
            "",
            "",
            False,
            id="Empty",
        ),
        pytest.param(
            "sample/pod5",
            ["sample/pod5/file.pod5"],
            "sample/final_summary.txt",
            "pod5_files_in_final_dest=1",
            True,
            id="Single_file",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/pod5/file1.pod5",
                "sample/pod5/file2.pod5",
            ],
            "sample/final_summary.txt",
            "pod5_files_in_final_dest=2",
            True,
            id="Multiple_files",
        ),
        pytest.param(
            "sample/pod5",
            ["sample/pod5/file.pod5"],
            "sample/final_summary.txt",
            "pod5_files_in_final_dest=2",
            False,
            id="Too few files",
        ),
    ],
)
def test_all_pod5_files_transfered(
    tmp_path: Path,
    pod5_dir: str,
    pod5_files: List[str],
    final_summary: str,
    final_summary_text: str,
    expected: bool,
):
    # Arrange

    # Insert tmp directory in path
    pod5_dir_path = tmp_path / pod5_dir
    pod5_files_path = [tmp_path / file for file in pod5_files]

    if final_summary:
        final_summary_path = tmp_path / final_summary
        final_summary_path.parent.mkdir(parents=True, exist_ok=True)
        final_summary_path.touch()
        final_summary_path.write_text(final_summary_text, encoding="utf-8")

    # Create files
    for pod5_file in pod5_files_path:
        pod5_file.parent.mkdir(parents=True, exist_ok=True)
        pod5_file.touch()

    # Act
    result = SequencingRun(pod5_dir_path).all_pod5_files_are_transferred()

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "pattern, dirs, expected",
    [
        pytest.param(
            "pattern",
            [],
            [],
            id="Empty",
        ),
        pytest.param(
            "dir",
            ["dir"],
            ["dir"],
            id="single_dir",
        ),
        pytest.param(
            "*/pod5",
            [
                "sample_1/pod5",
                "sample_2/pod5",
            ],
            [
                "sample_1/pod5",
                "sample_2/pod5",
            ],
            id="multiple_samples",
        ),
        pytest.param(
            "*/pod5*",
            [
                "sample/pod5_pass",
                "sample/pod5_fail",
            ],
            [
                "sample/pod5_pass",
                "sample/pod5_fail",
            ],
            id="Both pass and fail (suffix wildcard)",
        ),
        pytest.param(
            "*/pod5*",
            [
                "sample/pod5",
            ],
            [
                "sample/pod5",
            ],
            id="Single dir (suffix wildcard)",
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


@pytest.mark.usefixtures("mock_pod5_internals")
@pytest.mark.parametrize(
    "pod5_files, lock_files, done_files, expected",
    [
        pytest.param(
            [],
            [],
            [],
            [],
            id="Empty",
        ),
        pytest.param(
            ["file.pod5"],
            [],
            [],
            ["file.pod5"],
            id="Single pod5 file",
        ),
        pytest.param(
            ["file_1.pod5", "file_2.pod5"],
            [],
            [],
            ["file_1.pod5", "file_2.pod5"],
            id="Two pod5 files",
        ),
        pytest.param(
            ["file.pod5"],
            ["file.pod5.lock"],
            [],
            [],
            id="Skip pod5 file with lock file",
        ),
        pytest.param(
            ["file.pod5"],
            [],
            ["file.pod5.done"],
            [],
            id="Skip pod5 file with done file",
        ),
        pytest.param(
            ["file.pod5", "new_file.pod5"],
            ["file.pod5.lock"],
            [],
            ["new_file.pod5"],
            id="One pod5 done and one new pod5 file for basecalling",
        ),
    ],
)
def test_get_unbasecalled_pod5_files(tmp_path, pod5_files, lock_files, done_files, expected):
    # Arrange
    # Insert root dir
    pod5_dir = tmp_path / "pod5"
    pod5_files = [pod5_dir / file for file in pod5_files]
    create_files(pod5_files)

    run = SequencingRun(pod5_dir)

    lock_files = [run.basecalling_lock_files_dir / file for file in lock_files]
    done_files = [run.basecalling_done_files_dir / file for file in done_files]

    create_files(lock_files)
    create_files(done_files)

    # Act
    unbasecalled_files = run.get_unbasecalled_pod5_files()
    unbasecalled_files = [f.name for f in unbasecalled_files]

    # Assert
    assert set(unbasecalled_files) == set(expected)
