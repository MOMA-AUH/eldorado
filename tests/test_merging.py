import pytest

from eldorado.merging import are_all_files_basecalled, has_merge_lock_file
from eldorado.my_dataclasses import Pod5Directory


@pytest.mark.parametrize(
    "pod5_dir, final_summary, final_summary_text, pod5_files, done_files, expected",
    [
        pytest.param(
            "sample/pod5",
            "",
            "",
            [],
            [],
            False,
            id="empty",
        ),
        pytest.param(
            "sample/pod5",
            "sample/final_summary.txt",
            "pod5_files_in_final_dest=1",
            ["sample/pod5/file.pod5"],
            ["sample/bam_eldorado/done_files/file.pod5.done"],
            True,
            id="one_file",
        ),
        pytest.param(
            "sample/pod5",
            "sample/final_summary.txt",
            "pod5_files_in_final_dest=2",
            ["sample/pod5/file.pod5"],
            ["sample/bam_eldorado/done_files/file.pod5.done"],
            False,
            id="too_few_files",
        ),
    ],
)
def test_is_done_basecalling(
    tmp_path,
    pod5_dir,
    final_summary,
    final_summary_text,
    pod5_files,
    done_files,
    expected,
):
    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Create files
    pod5_dir = root_dir / pod5_dir
    pod5_dir.mkdir(parents=True, exist_ok=True)
    if final_summary:
        final_summary = root_dir / final_summary
        final_summary.touch()
        final_summary.write_text(final_summary_text, encoding="utf-8")
    for pod5_file in pod5_files:
        pod5_file = root_dir / pod5_file
        pod5_file.parent.mkdir(parents=True, exist_ok=True)
        pod5_file.touch()
    for done_file in done_files:
        done_file = root_dir / done_file
        done_file.parent.mkdir(parents=True, exist_ok=True)
        done_file.touch()

    # Act
    pod5_dir = Pod5Directory(pod5_dir)
    result = are_all_files_basecalled(pod5_dir=pod5_dir)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "pod5_dir, merge_lock_file, expected",
    [
        pytest.param(
            "sample/pod5",
            "sample/bam_eldorado/merge.lock",
            True,
            id="lock_file_exists",
        ),
        pytest.param(
            "sample/pod5",
            "",
            False,
            id="lock_file_missing",
        ),
    ],
)
def test_has_merge_lock_file(
    tmp_path,
    pod5_dir,
    merge_lock_file,
    expected,
):
    # Arrange
    root_dir = tmp_path / "root"
    root_dir.mkdir()

    # Create files
    pod5_dir = root_dir / pod5_dir
    pod5_dir.mkdir(parents=True, exist_ok=True)
    if merge_lock_file:
        merge_lock_file = root_dir / merge_lock_file
        merge_lock_file.parent.mkdir(parents=True, exist_ok=True)
        merge_lock_file.touch()

    # Act
    pod5_dir = Pod5Directory(pod5_dir)
    result = has_merge_lock_file(pod5_dir)

    # Assert
    assert result == expected
