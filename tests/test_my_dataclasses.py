from pathlib import Path
from unittest.mock import MagicMock

from typing import List

import pytest

import eldorado.my_dataclasses as my_dataclasses
from eldorado.my_dataclasses import is_file_inactive, Pod5Directory


@pytest.mark.parametrize(
    "time_created, time_now, min_time, expected",
    [
        pytest.param(0, 6, 5, True, id="done"),
        pytest.param(0, 4, 5, False, id="not_done"),
    ],
)
def test_is_file_inactive(monkeypatch, time_created, time_now, min_time, expected):
    # Arrange
    monkeypatch.setattr(my_dataclasses.time, "time", lambda *args, **kwargs: time_now)
    monkeypatch.setattr(my_dataclasses.Path, "stat", lambda *args, **kwargs: MagicMock(st_mtime=time_created))

    # Act
    result = is_file_inactive(Path("file"), min_time)

    # Assert
    assert result == expected


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
    monkeypatch,
    pod5_dir: str,
    pod5_files: List[str],
    final_summary: str,
    final_summary_text: str,
    expected: bool,
):
    # Arrange
    # Mock is_file_inactive
    def mock_is_file_inactive(*args, **kwargs):
        return True

    monkeypatch.setattr(my_dataclasses, "is_file_inactive", mock_is_file_inactive)

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
    result = Pod5Directory(pod5_dir_path).all_pod5_files_transfered()

    # Assert
    assert result == expected
