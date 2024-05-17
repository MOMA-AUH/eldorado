import subprocess
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
