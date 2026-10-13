"""Read and verify the accepted historical data layout without running its code."""

import hashlib
from pathlib import Path

import numpy as np

from .storage import digest, read


def safe(root, relative):
    p = (root / relative).resolve()
    if root.resolve() not in p.parents:
        raise ValueError("Archive path escapes its root")
    return p


def verify(run, results, summary):
    for name, base in [
        ("source-sha256.json", run / "source"),
        ("code-sha256.json", run),
    ]:
        for relative, expected in read(run / name).items():
            if digest(safe(base, relative)) != expected:
                raise ValueError("Preserved evidence changed: " + relative)
    manifest = read(run / "manifest.json")
    # Resolve source directories against preserved copies, never against old absolute locations.
    old_root = Path(manifest["extension_of"])
    for j in results["jobs"]:
        if j["status"] != "completed":
            continue
        directory = Path(j["directory"])
        if directory.is_relative_to(old_root):
            location = safe(run / "source", directory.relative_to(old_root))
        else:
            # New extension artifacts live directly under jobs, identified by saved job ID.
            location = safe(run, Path("jobs") / j["id"])
        with np.load(location / "rows.npz", allow_pickle=False) as a:
            if "counts" in a.files:
                counts, offsets, rows = a["counts"], a["offsets"], a["rows"]
                if (
                    len(offsets) != len(counts) + 1
                    or offsets[-1] != len(rows)
                    or not np.array_equal(np.diff(offsets), counts)
                ):
                    raise ValueError("Historical output offsets differ")
                arrays = [rows[offsets[i] : offsets[i + 1]] for i in range(len(counts))]
                hashes = np.asarray(
                    [hashlib.sha256(x.tobytes()).hexdigest() for x in arrays],
                    dtype="S64",
                )
                if (
                    hashlib.sha256(counts.tobytes()).hexdigest() != j["counts_sha256"]
                    or hashlib.sha256(hashes.tobytes()).hexdigest() != j["rows_sha256"]
                ):
                    raise ValueError("Historical window fingerprints differ")
                if hashlib.sha256(
                    b"".join(x[:, :2].tobytes() for x in arrays)
                ).hexdigest() != j["coordinates_sha256"] or not np.all(
                    np.abs(rows[:, 3]) <= 5 * 86400 + 1e-6
                ):
                    raise ValueError("Historical coordinates/time window differ")
            else:
                arrays = [a[f"point_{i}"] for i in range(j["points"])]
                if [len(x) for x in arrays] != j["counts"] or [
                    hashlib.sha256(x.tobytes()).hexdigest() for x in arrays
                ] != j["row_hashes"]:
                    raise ValueError("Historical all-record fingerprints differ")

    def key(r):
        return (r["episode"], r.get("date"), r.get("batch_position"))

    jobs = {key(j): j for j in results["jobs"] if j["trial"] == 0}
    for case in summary["exploratory_cases"]:
        original = jobs[key(case)]
        if case["status"] != original["status"] or case.get("seconds") != original.get(
            "seconds"
        ):
            raise ValueError("Historical case summary differs from saved measurements")
    byid = {j["id"]: j for j in results["jobs"]}
    episode_jobs = {
        e["id"]: [byid[j["id"]] for j in e["jobs"]] for e in results["episodes"]
    }
    for row in summary["episodes"]:
        js = episode_jobs[row["id"]]
        complete = all(j["status"] == "completed" for j in js)
        if complete != row["complete"] or row.get("seconds") != (
            sum(j["seconds"] for j in js) if complete else None
        ):
            raise ValueError("Historical episode total differs")
