import subprocess
import os
from pathlib import Path


def is_in_queue(job_id: str):
    if not job_id:
        return False

    res = subprocess.run(
        ["squeue", "--job", job_id],
        check=False,
        capture_output=True,
    )
    return res.returncode == 0


def write_to_file(file_path: Path, content: str):
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)


def is_complete_pod5_file(path: Path) -> bool:
    # Pod5 docs: https://pod5-file-format.readthedocs.io/en/latest/SPECIFICATION.html#combined-file-layout
    pattern = bytes((0x8B, 0x50, 0x4F, 0x44, 0xD, 0xA, 0x1A, 0x0A))
    pattern_len = len(pattern)

    fd = os.open(path, os.O_RDONLY)
    try:
        # Check if the file starts with the pattern
        header = os.read(fd, pattern_len)
        if header != pattern:
            return False

        # Check if the file ends with the pattern
        os.lseek(fd, -pattern_len, os.SEEK_END)
        footer = os.read(fd, pattern_len)
        return footer == pattern
    finally:
        os.close(fd)
