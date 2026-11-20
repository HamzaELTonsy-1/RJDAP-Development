import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
LOGS = ROOT / "logs"

HOST = "127.0.0.1"
PORT = 5000
WORKER_COUNT = 4
TASK_COUNT = 1000


def main():
    LOGS.mkdir(exist_ok=True)

    processes = []
    log_files = []

    try:
        server_log = open(LOGS / "server.log", "w")
        log_files.append(server_log)

        server = subprocess.Popen(
            [
                sys.executable,
                str(SRC / "server.py"),
                "--host", HOST,
                "--port", str(PORT),
                "--workers", str(WORKER_COUNT),
                "--tasks", str(TASK_COUNT),
            ],
            stdout=server_log,
            stderr=subprocess.STDOUT,
        )
        processes.append(server)

        time.sleep(1)

        if server.poll() is not None:
            raise RuntimeError("Server stopped before workers started.")

        for worker_id in range(1, WORKER_COUNT + 1):
            worker_log = open(LOGS / f"worker_{worker_id}.log", "w")
            log_files.append(worker_log)

            worker = subprocess.Popen(
                [
                    sys.executable,
                    str(SRC / "worker.py"),
                    "--id", str(worker_id),
                    "--host", HOST,
                    "--port", str(PORT),
                ],
                stdout=worker_log,
                stderr=subprocess.STDOUT,
            )
            processes.append(worker)

        print("Started server and four workers.")
        print("Waiting for the 1,000-task baseline test...")

        server.wait(timeout=180)

        print("Server finished. Checking baseline summary...")

        summary = (LOGS / "server.log").read_text(errors="replace")
        print(summary[-1500:])

        if "Tasks completed: 1000" not in summary:
            raise RuntimeError("Baseline did not complete all 1,000 tasks.")

        if "Duplicate results detected: 0" not in summary:
            raise RuntimeError("Baseline duplicate-result check failed.")

        print("BASELINE TEST PASSED.")

    except Exception as error:
        print(f"BASELINE TEST FAILED: {error}")
        raise

    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()

        for process in processes:
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

        for log_file in log_files:
            log_file.close()


if __name__ == "__main__":
    main()
