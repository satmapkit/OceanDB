"""One-use batches in a persistent worker; narrowly scoped cancellation."""

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from .database import connect
from .storage import claim, event, now, read, save


def cancel_owned(database, application):
    if not application.startswith("ob_batch_"):
        raise ValueError("Refusing unscoped cancellation")
    with connect() as c:
        return c.execute(
            "SELECT pid,pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname=%s AND application_name=%s",
            (database, application),
        ).fetchall()


class Worker:
    def __init__(self, run, manifest):
        self.run = Path(run)
        self.manifest = manifest
        self.proc = None
        self.log = None
        self.generation = 0

    def start(self):
        if self.proc and self.proc.poll() is None:
            return
        self.generation += 1
        ready = self.run / f"worker-{self.generation}.ready"
        if ready.exists():
            raise ValueError("Worker identity already used")
        self.log = (self.run / f"worker-{self.generation}.log").open("w")
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-B",
                "-m",
                "OceanDB.benchmark.worker",
                str(self.run),
                str(ready),
            ],
            stdin=subprocess.PIPE,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        deadline = time.monotonic() + 60
        while (
            not ready.exists()
            and self.proc.poll() is None
            and time.monotonic() < deadline
        ):
            time.sleep(0.02)
        if not ready.exists():
            raise RuntimeError("Worker startup failed")

    def stop_process(self):
        if self.proc and self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGTERM)
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait(timeout=5)
        if self.proc and self.proc.stdin:
            self.proc.stdin.close()
        if self.log:
            self.log.close()

    def cancel(self, job):
        database = self.manifest["configurations"][job["configuration"]]["database"]
        cancelled = cancel_owned(database, job["application_name"])
        self.stop_process()
        with connect() as c:
            c.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s AND application_name=%s",
                (database, job["application_name"]),
            ).fetchall()
            if c.execute(
                "SELECT 1 FROM pg_stat_activity WHERE datname=%s AND application_name=%s",
                (database, job["application_name"]),
            ).fetchone():
                raise RuntimeError("Worker session cleanup failed")
        return cancelled

    def execute(self, job, before_go=None):
        self.start()
        p = self.run / "jobs" / job["id"]
        p.mkdir(parents=True, exist_ok=False)
        job = dict(
            job,
            directory=str(p.relative_to(self.run)),
            application_name="ob_batch_" + uuid.uuid4().hex,
        )
        save(p / "job.json", job)
        try:
            self.proc.stdin.write(json.dumps(job) + "\n")
            self.proc.stdin.flush()
            deadline = time.monotonic() + 60
            while (
                not (p / "ready.json").exists()
                and not (p / "failure.json").exists()
                and self.proc.poll() is None
                and time.monotonic() < deadline
            ):
                time.sleep(0.02)
            if not (p / "ready.json").exists():
                raise RuntimeError("Worker initialization failed; inspect " + str(p))
            if before_go:
                before_go()
            claim(p / "go.json", dict(time=now(), may_have_started=True))
            event(
                self.run,
                "batch_started",
                id=job["id"],
                configuration=job["configuration"],
                scenario=job["scenario"],
            )
            start = time.monotonic()
            self.proc.stdin.write("go\n")
            self.proc.stdin.flush()
            while (
                self.proc.poll() is None
                and not (p / "timing.json").exists()
                and not (p / "failure.json").exists()
                and time.monotonic() - start < job["cap_seconds"]
            ):
                time.sleep(0.01)
            if (p / "timing.json").exists():
                deadline = time.monotonic() + 120
                while (
                    not (p / "result.json").exists()
                    and not (p / "failure.json").exists()
                    and self.proc.poll() is None
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.02)
                if (p / "result.json").exists():
                    result = read(p / "result.json")
                elif (p / "failure.json").exists():
                    result = read(p / "failure.json")
                else:
                    raise RuntimeError("Post-timing fingerprint stage failed")
                if result.get("seconds", 0) > job["cap_seconds"]:
                    result = dict(
                        status="timed_out",
                        observed_completion_seconds=result["seconds"],
                    )
            elif (p / "failure.json").exists():
                result = read(p / "failure.json")
            elif time.monotonic() - start >= job["cap_seconds"]:
                result = dict(status="timed_out")
            else:
                result = dict(status="failed", error_type="WorkerExited")
            if result["status"] != "completed":
                self.cancel(job)
            out = dict(job, **result, worker_generation=self.generation)
            save(p / "supervisor.json", out)
            return out
        except BaseException:
            self.cancel(job)
            raise

    def close(self):
        if self.proc and self.proc.poll() is None:
            self.proc.stdin.close()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.stop_process()
        if self.log:
            self.log.close()
