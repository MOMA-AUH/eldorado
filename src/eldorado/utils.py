import subprocess


def is_in_queue(job_id):
    res = subprocess.run(
        ["squeue", "--job", str(job_id)],
        check=False,
        capture_output=True,
    )
    return res.returncode == 0
