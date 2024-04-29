import pytest

from eldorado import pod5_handling


@pytest.fixture
def mock_pod5_internals(monkeypatch):
    monkeypatch.setattr(pod5_handling, "is_file_inactive", lambda *args, **kwargs: True)
    monkeypatch.setattr(pod5_handling, "get_metadata", lambda *args, **kwargs: None)
