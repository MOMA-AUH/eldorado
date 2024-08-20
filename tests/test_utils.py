import pytest

from eldorado.utils import is_complete_pod5_file


@pytest.mark.parametrize(
    "file_content, expected",
    [
        pytest.param(
            b"\x8BPOD\r\n\x1A\n",
            True,
            id="valid_pod5_file",
        ),
        pytest.param(
            b"\x8BPOD\r\n\x1A",
            False,
            id="incomplete_pod5_file",
        ),
        pytest.param(
            b"\x8BPOD\r\n",
            False,
            id="missing_eof",
        ),
        pytest.param(
            b"\x8BPOD\r\n\x1A\n\x00\x8BPOD\r\n\x1A\n",
            True,
            id="file_with_extra_data",
        ),
        pytest.param(
            b"\x8BPOD\r\n\x1A\n\x00",
            False,
            id="incomplete_file_with_extra_data",
        ),
    ],
)
def test_is_complete_pod5_file(tmp_path, file_content, expected):
    # Arrange
    file_path = tmp_path / "file.pod5"
    with open(file_path, "wb") as f:
        f.write(file_content)
    # Act
    result = is_complete_pod5_file(file_path)
    # Assert
    assert result == expected
