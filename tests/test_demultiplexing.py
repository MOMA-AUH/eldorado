import pytest

from pathlib import Path

from typing import List

from eldorado.demultiplexing import demultiplexing_is_pending
from eldorado.pod5_handling import SequencingRun


@pytest.mark.usefixtures("mock_pod5_internals")
@pytest.mark.parametrize(
    "pod5_dir, existing_files, expected",
    [
        pytest.param(
            "pod5",
            [],
            False,
            id="Empty",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/merging/merge.done",
                "bam_eldorado/merging/merged.bam",
                "bam_eldorado/dorado_config.json",
            ],
            True,
            id="Merge done",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/merging/merged.bam",
                "bam_eldorado/dorado_config.json",
            ],
            False,
            id="Merge merge done file missing",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/merging/merge.done",
                "bam_eldorado/dorado_config.json",
            ],
            False,
            id="Merge merged bam missing",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/merging/merge.done",
                "bam_eldorado/merging/merged.bam",
            ],
            False,
            id="Merge dorado config missing",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/demultiplexing/demux.lock",
            ],
            False,
            id="Demultiplexing lock",
        ),
        pytest.param(
            "pod5",
            [
                "pod5/file.pod5",
                "bam_eldorado/demultiplexing/demux.done",
            ],
            False,
            id="Demultiplexing done",
        ),
    ],
)
def test_needs_demultiplexing(tmp_path: Path, pod5_dir: Path, existing_files: List[Path], expected: bool):

    # Arrange
    pod5_dir = tmp_path / pod5_dir
    existing_files = [tmp_path / f for f in existing_files]

    for f in existing_files:
        f.parent.mkdir(parents=True, exist_ok=True)
        f.touch()

    # Act
    result = demultiplexing_is_pending(SequencingRun(pod5_dir))

    # Assert
    assert result == expected
