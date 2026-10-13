"""One-use fixed schedule. Only untimed preparation attempts may repeat."""

import copy
import traceback
from collections import Counter
from pathlib import Path

import numpy as np

from .cache import Cache
from .database import credentials, inventory
from .executor import Worker
from .storage import claim, digest, event, external, now, read, save, verify_code
from .validation import summarize, validate


def audit(run, plan, results):
    expected = [j["id"] for e in plan["episodes"] for j in e["jobs"]]
    actual = [j["id"] for j in results["jobs"]]
    if len(set(actual)) != len(actual) or expected != actual:
        raise ValueError("Schedule accounting differs")
    if any(
        j["status"] not in ("completed", "timed_out", "skipped")
        for j in results["jobs"]
    ):
        raise ValueError("Nonterminal schedule records")
    for j in results["jobs"]:
        if j["status"] == "completed":
            with np.load(run / j["directory"] / "rows.npz", allow_pickle=False) as rows:
                detail = summarize([rows[f"point_{i}"] for i in range(j["points"])], j)
                if any(
                    j[k] != detail[k]
                    for k in ("sha256", "counts", "coordinates_sha256")
                ):
                    raise ValueError("Saved output fingerprint differs")
    planned = {j["id"]: j for e in plan["episodes"] for j in e["jobs"]}
    for j in results["jobs"]:
        if any(j[k] != v for k, v in planned[j["id"]].items() if k != "status"):
            raise ValueError("Immutable request fields changed")
        started = (run / "jobs" / j["id"] / "go.json").exists()
        if started != (j["status"] in ("completed", "timed_out")):
            raise ValueError("Start markers differ from terminal request statuses")
    for relative, expected_hash in read(run / "code-sha256.json").items():
        if digest(run / "code" / relative) != expected_hash:
            raise ValueError("Frozen source copy changed")
    cache = []
    for e in plan["episodes"]:
        r = read(run / "cache" / f"{e['id']}.json")
        if (
            r["status"] != "verified"
            or r["postgres_verified"]
            or r["os_verified"]["after_pages"]
        ):
            raise ValueError("Unverified cache start")
        cache.append(
            dict(
                episode=e["id"],
                configuration=e["configuration"],
                segment="prepared_run",
                postgres_verified=0,
                os_verified_pages=0,
                seconds=r["preparation_seconds"],
            )
        )
    verify_code(run)
    validation = validate(results["jobs"])
    save(run / "validation.json", validation)
    if validation["status"] == "failed":
        raise ValueError("Correctness mismatch")
    save(
        run / "verification.json",
        dict(
            status="passed",
            unique_ids=len(actual),
            cache_verification=cache,
            validation_status=validation["status"],
            frozen_files=len(read(run / "code-sha256.json")),
        ),
    )


def run(output):
    path = external(output)
    m = read(path / "manifest.json")
    plan = read(path / "plan.json")
    if m["status"] != "ready":
        raise ValueError("Prepared run is not ready")
    verify_code(path)
    if digest(path / "plan.json") != m["plan_sha256"]:
        raise ValueError("Frozen plan changed")
    if {k: v for k, v in credentials().items() if k != "password"} != m["connection"]:
        raise ValueError("Connection identity differs from prepared run")
    if read(path / "cache-self-test.json")["status"] != "passed":
        raise ValueError("Cache canary proof missing")
    for inv in m["configurations"].values():
        current = inventory(inv["database"], m["owner_token"])
        for key in (
            "oid",
            "owner",
            "owner_token",
            "indexes",
            "relations",
            "statistics_sha256",
            "dataset_sha256",
        ):
            # JSON normalizes tuples to lists.
            import json

            if json.dumps(current[key], default=str) != json.dumps(
                inv[key], default=str
            ):
                raise ValueError("Prepared inventory changed: " + key)
    claim(
        path / "run-started.json",
        dict(time=now(), plan_sha256=digest(path / "plan.json")),
    )
    result = dict(
        schema_version=1,
        run_id=m["run_id"],
        status="running",
        started_at=now(),
        episodes=plan["episodes"],
        excluded=plan["excluded"],
        jobs=copy.deepcopy([j for e in plan["episodes"] for j in e["jobs"]]),
    )
    positions = {j["id"]: i for i, j in enumerate(result["jobs"])}
    cache = Cache(path, m)

    def checkpoint():
        result["statuses"] = dict(Counter(j["status"] for j in result["jobs"]))
        save(path / "results.json", result)

    checkpoint()
    active = None
    worker = Worker(path, m)
    try:
        for e in plan["episodes"]:
            result.update(active_stage=e["stage"], active_episode=e["id"])
            checkpoint()
            interrupted = False
            for i, j in enumerate(e["jobs"]):
                active = j
                result["active_batch"] = j["id"]
                checkpoint()
                if interrupted:
                    rec = dict(
                        j, status="skipped", reason="Earlier batch timed out; no retry"
                    )
                else:
                    rec = worker.execute(
                        j,
                        (
                            (lambda: cache.reset(e["configuration"], e["id"]))
                            if i == 0
                            else None
                        ),
                    )
                result["jobs"][positions[j["id"]]] = rec
                checkpoint()
                if rec["status"] == "failed":
                    raise RuntimeError("Saved worker failure: " + j["id"])
                interrupted = rec["status"] != "completed"
            active = None
            validation = validate(result["jobs"])
            save(path / "validation.json", validation)
            if validation["status"] == "failed":
                raise ValueError("Exact outputs differ; see validation.json")
            event(
                path, "episode_completed", episode=e["id"], statuses=result["statuses"]
            )
        worker.close()
        result.update(
            status="timings_complete",
            active_stage=None,
            active_episode=None,
            active_batch=None,
        )
        checkpoint()
        audit(path, plan, result)
        from .diagnostics import diagnostics

        diagnostics(path, m, plan)
        result["status"] = "completed"
        checkpoint()
        event(path, "completed", statuses=result["statuses"])
    except BaseException as exc:
        result["status"] = (
            "cancelled" if isinstance(exc, KeyboardInterrupt) else "failed"
        )
        if active and result["jobs"][positions[active["id"]]]["status"] == "pending":
            result["jobs"][positions[active["id"]]]["status"] = (
                "cancelled" if isinstance(exc, KeyboardInterrupt) else "failed"
            )
        checkpoint()
        (path / "pipeline-exception.txt").write_text(traceback.format_exc())
        event(path, result["status"], exception_type=type(exc).__name__)
        raise
    finally:
        worker.close()
    return result
