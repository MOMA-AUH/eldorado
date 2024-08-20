import pytest

from eldorado import pod5_handling


def create_files(files):
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()


@pytest.fixture
def mock_pod5_internals(monkeypatch):
    monkeypatch.setattr(pod5_handling, "is_complete_pod5_file", lambda *args, **kwargs: True)
    monkeypatch.setattr(pod5_handling, "get_metadata", lambda *args, **kwargs: None)
