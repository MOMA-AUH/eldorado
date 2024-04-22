import subprocess

from pathlib import Path

from typing import List

from eldorado.my_dataclasses import Pod5Directory


def find_pod5_dirs_for_processing(root_dir: Path, pattern: str) -> List[Pod5Directory]:

    # Get all pod5 directories that match the pattern
    pod5_dirs = get_pod5_dirs_from_pattern(root_dir, pattern)

    # Keep only pod5 directories that are not already basecalled
    pod5_dirs = [x for x in pod5_dirs if needs_basecalling(x)]

    # Keep only pod5 directories that has pod5 files
    pod5_dirs = [x for x in pod5_dirs if contains_pod5_files(x)]

    # Return as Pod5Directory objects
    return [Pod5Directory(x) for x in pod5_dirs]


def get_pod5_dirs_from_pattern(root_dir: Path, pattern: str) -> List[Path]:
    return list(root_dir.glob(pattern=pattern))


def needs_basecalling(pod5_dir: Path) -> bool:
    return not any(pod5_dir.parent.glob("bam*/*.bam")) and not any(pod5_dir.parent.glob("fastq*/*.fastq*"))


def contains_pod5_files(x: Path) -> bool:
    return any(x.glob("*.pod5"))


def is_in_queue(job_id):
    res = subprocess.run(
        ["squeue", "--job", str(job_id)],
        check=False,
        capture_output=True,
    )
    return res.returncode == 0
