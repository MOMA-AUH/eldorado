import pytest

from pathlib import Path

from typing import List

from eldorado.demultiplexing import demultiplexing_is_pending, sample_sheet_is_valid
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


@pytest.mark.parametrize(
    "sample_sheet_text, expected",
    [
        pytest.param(
            "barcode,alias\n",
            True,
            id="Valid sample sheet",
        ),
        pytest.param(
            "barcode\n",
            False,
            id="Missing alias",
        ),
        pytest.param(
            "alias\n",
            False,
            id="Missing barcode",
        ),
        pytest.param(
            "",
            False,
            id="Empty",
        ),
        pytest.param(
            "barcode,alias\nA1,Sample1\n",
            True,
            id="Single line",
        ),
        pytest.param(
            "barcode,alias\nA1,Sample1\nA2,Sample2\n",
            True,
            id="Multiple lines",
        ),
        pytest.param(
            "barcode,alias\nA1,Sample1\nA2,Sample1\n",
            False,
            id="Non-unique alias",
        ),
        pytest.param(
            "barcode,alias\nA1,Too long alias that is more than 40 characters\n",
            False,
            id="Too long alias",
        ),
    ],
)
def test_sample_sheet_has_alias_and_barcode(tmp_path: Path, sample_sheet_text: str, expected: bool):
    # Act
    # Create sample sheet
    sample_sheet = tmp_path / "sample_sheet.csv"
    sample_sheet.write_text(sample_sheet_text)

    # Check if sample sheet is valid
    result = sample_sheet_is_valid(sample_sheet)

    # Assert
    assert result == expected
