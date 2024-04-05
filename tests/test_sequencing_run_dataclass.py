from pathlib import Path
from unittest.mock import MagicMock

import pytest

import src.eldorado.my_dataclasses as my_dataclasses
from src.eldorado.my_dataclasses import file_is_done_transfering


@pytest.mark.parametrize(
    "time_created, time_now, min_time, expected",
    [
        pytest.param(0, 6, 5, True, id="done"),
        pytest.param(0, 4, 5, False, id="not_done"),
    ],
)
def test_is_done_transfering(monkeypatch, time_created, time_now, min_time, expected):
    # Arrange
    monkeypatch.setattr(my_dataclasses.time, "time", lambda *args, **kwargs: time_now)
    monkeypatch.setattr(my_dataclasses.Path, "stat", lambda *args, **kwargs: MagicMock(st_mtime=time_created))

    # Act
    result = file_is_done_transfering(Path("file"), min_time)

    # Assert
    assert result == expected
