import pytest

from typing import List

from pathlib import Path

from eldorado.cleanup import needs_cleanup, load_logs_as_dicts, generate_final_log_csv
from eldorado.basecalling import BasecallingRun


@pytest.mark.parametrize(
    "pod5_dir, files, expected",
    [
        pytest.param(
            "sample/pod5",
            [],
            False,
            id="Empty",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/demultiplexing/demux.done",
            ],
            False,
            id="Single dir with done file",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/demultiplexing/demux.lock",
            ],
            False,
            id="Single dir with lock file",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/final.bam",
            ],
            False,
            id="Single dir with final BAM, but no done file",
        ),
        pytest.param(
            "sample/pod5",
            [
                "sample/bam_eldorado/demultiplexing/demux.done",
                "sample/bam_eldorado/final.bam",
            ],
            True,
            id="Single dir with done file and final BAM",
        ),
    ],
)
def test_needs_cleanup(tmp_path: Path, pod5_dir: Path, files: List[Path], expected: bool):
    # Arrange
    pod5_dir = Path(tmp_path / pod5_dir)
    files = [Path(tmp_path / file) for file in files]

    pod5_dir.mkdir(parents=True)

    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()

    # Act
    run = BasecallingRun(pod5_dir)
    result = needs_cleanup(run)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "log_texts, expected",
    [
        pytest.param(
            [],
            [],
            id="Empty",
        ),
        pytest.param(
            ["key1=value1\nkey2=value2"],
            [
                {
                    "key1": "value1",
                    "key2": "value2",
                },
            ],
            id="Single log file",
        ),
        pytest.param(
            [
                "key1=value1\nkey2=value2",
                "key3=value3\nkey4=value4",
            ],
            [
                {
                    "key1": "value1",
                    "key2": "value2",
                },
                {
                    "key3": "value3",
                    "key4": "value4",
                },
            ],
            id="Multiple log files",
        ),
    ],
)
def test_load_logs_as_dicts(tmp_path, log_texts, expected):
    # Arrange
    log_files = []
    for i, content in enumerate(log_texts):
        log_file = Path(tmp_path / f"log{i}.log")
        log_file.write_text(content, encoding="utf-8")
        log_files.append(log_file)

    # Act
    result = load_logs_as_dicts(log_files)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    "logs, expected",
    [
        pytest.param(
            [],
            "\n",
            id="Empty",
        ),
        pytest.param(
            [
                {
                    "key1": "value1",
                    "key2": "value2",
                },
            ],
            "key1,key2\nvalue1,value2\n",
            id="Single log",
        ),
        pytest.param(
            [
                {
                    "key1": "value1",
                },
                {
                    "key1": "value2",
                },
            ],
            "key1\nvalue1\nvalue2\n",
            id="Multiple logs, same key",
        ),
        pytest.param(
            [
                {
                    "key1": "value1",
                },
                {
                    "key2": "value2",
                },
            ],
            "key1,key2\nvalue1,\n,value2\n",
            id="Multiple logs, different keys",
        ),
    ],
)
def test_generate_final_log_csv(tmp_path, logs, expected):
    # Arrange
    csv_file = Path(tmp_path / "output.csv")

    # Act
    generate_final_log_csv(csv_file, logs)

    # Assert
    assert csv_file.read_text(encoding="utf-8") == expected
