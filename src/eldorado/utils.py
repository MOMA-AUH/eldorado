import subprocess


def is_in_queue(job_id: str):
    if not job_id:
        return False

    res = subprocess.run(
        ["squeue", "--job", job_id],
        check=False,
        capture_output=True,
    )
    return res.returncode == 0
