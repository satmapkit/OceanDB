"""Exact per-center rows; NN value differences require identical coordinates."""

import hashlib
import json
from collections import defaultdict

import numpy as np

from .workloads import canonical_record


def canonical_window(values):
    if values is None:
        return np.empty((0, 4), dtype="<f8")
    rows = np.column_stack(values).astype("<f8")
    rows[rows == 0] = 0
    rows[np.isnan(rows)] = np.nan
    return np.ascontiguousarray(
        rows[np.lexsort(tuple(rows[:, k] for k in (3, 2, 1, 0)))]
    )


def summarize(arrays, job):
    record = job["temporal_scope"] == "entire_stored_record"
    counts = [len(a) for a in arrays]
    hashes = [hashlib.sha256(a.tobytes()).hexdigest() for a in arrays]
    if not record and any(
        not np.all(np.abs(a[:, 3]) <= job["half_window_days"] * 86400 + 1e-6)
        for a in arrays
    ):
        raise ValueError("Returned rows exceed the requested temporal window")
    if job["scenario"].startswith("nn") and any(n > 3 for n in counts):
        raise ValueError("NN3 returned more than three rows")
    coords = []
    for a in arrays:
        xy = np.column_stack((a["longitude"], a["latitude"])) if record else a[:, :2]
        xy = xy[np.lexsort((xy[:, 1], xy[:, 0]))] if len(xy) else xy
        coords.append(hashlib.sha256(np.ascontiguousarray(xy).tobytes()).hexdigest())
    return dict(
        points=len(arrays),
        rows=sum(counts),
        counts=counts,
        row_hashes=hashes,
        coordinates_sha256=hashlib.sha256(json.dumps(coords).encode()).hexdigest(),
        sha256=hashlib.sha256(
            json.dumps(dict(counts=counts, hashes=hashes), sort_keys=True).encode()
        ).hexdigest(),
        within_window=True,
        canonical_bytes=sum(a.nbytes for a in arrays),
    )


def save_rows(values, job, path):
    canonical = (
        canonical_record
        if job["temporal_scope"] == "entire_stored_record"
        else canonical_window
    )
    arrays = [canonical(v) for v in values]
    if len(arrays) != job["points"]:
        raise ValueError("Missing query center outputs")
    detail = summarize(arrays, job)
    np.savez_compressed(path, **{f"point_{i}": a for i, a in enumerate(arrays)})
    return detail


def validate(jobs):
    groups = defaultdict(list)
    for j in jobs:
        if j["status"] == "completed":
            groups[
                (j["workflow"], j["scenario"], j["date"], j["batch_position"])
            ].append(j)
    out = []
    for key, rows in groups.items():
        nn = key[1].startswith("nn")
        same_counts = len({tuple(r["counts"]) for r in rows}) == 1
        same_coords = len({r["coordinates_sha256"] for r in rows}) == 1
        same_rows = len({r["sha256"] for r in rows}) == 1
        valid = same_counts and same_coords and (nn or same_rows)
        out.append(
            dict(
                workflow=key[0],
                scenario=key[1],
                date=key[2],
                batch_position=key[3],
                records=len(rows),
                valid=valid,
                nn_same_coordinate_value_qualification=nn and valid and not same_rows,
            )
        )
    status = (
        "failed"
        if any(not r["valid"] for r in out)
        else (
            "passed_with_qualifications"
            if any(r["nn_same_coordinate_value_qualification"] for r in out)
            else "passed" if out else "unmeasured"
        )
    )
    return dict(
        status=status,
        groups=out,
        qualification="NN differences are qualified only when selected coordinates and counts match exactly; all radius values must match.",
    )
