"""Core and exhaustive profiles use the same fixed episode protocol."""

import hashlib
import itertools
import json
import random
from collections import Counter
from datetime import date, timedelta

import numpy as np

from OceanDB.ocean_data.basins import BasinMask

from .workloads import RECORD_FIELDS, WINDOW_FIELDS, local_grid

COLUMNS = dict(a="along_track_point", b="basin_id", d="date_time", m="mission")
CORE = ("ad", "amd", "bad", "abd", "bma", "bmad", "mba")


def configurations(profile):
    names = (
        list(CORE)
        if profile == "core"
        else [
            "".join(p)
            for n in (2, 3, 4)
            for p in itertools.permutations("abdm", n)
            if p[0] != "d"
            and not ("a" in p and "d" in p and p.index("d") < p.index("a"))
        ]
    )
    return {
        label: [COLUMNS[k] for k in ("ad" if label.startswith("ad_") else label)]
        for label in ["ad_a", "ad_b", *(n for n in names if n != "ad")]
    }


def primary_points(seed, count):
    lat, lon = np.meshgrid(
        np.arange(-60, 60, 1), np.arange(-180, 180, 1), indexing="ij"
    )
    mask = BasinMask()
    ocean = [
        (float(a), float(b))
        for a, b in zip(lat.ravel(), lon.ravel())
        if mask.loc_is_ocean(a, b)
    ]
    indices = sorted(random.Random(seed).sample(range(len(ocean)), count))
    return [ocean[i] for i in indices]


def build(cfg):
    samples = dict(
        primary=primary_points(cfg["sample_seed"], cfg["points"]),
        local_grid=local_grid(
            cfg["center"], cfg["rows"], cfg["columns"], cfg["spacing_m"]
        ),
    )
    mask = BasinMask()
    if not all(mask.loc_is_ocean(*p) for p in samples["local_grid"]):
        raise ValueError("Local patch includes land; do not silently thin the grid")
    dates = [
        (date.fromisoformat(cfg["daily_start"]) + timedelta(days=i)).isoformat()
        for i in range(cfg["daily_days"])
    ]
    groups = [
        (w, ds, s)
        for w, ds in [
            ("daily_sequence", dates),
            ("isolated_interior", [cfg["interior_date"]]),
            ("isolated_boundary", [cfg["boundary_date"]]),
        ]
        for s in ("radius_all", "nn_all", "radius_filtered", "nn_filtered")
    ]
    groups += [
        ("all_record_local_grid", [], s)
        for s in ("all_record_radius_all", "all_record_radius_filtered")
    ]
    configs = configurations(cfg["profile"])
    episodes, excluded = [], []
    matched = set()

    def add(label, trial, group, stage, order=None, position=None):
        workflow, ds, scenario = groups[group]
        for ri, rule in enumerate(cfg["exclusions"]):
            if (
                label in rule["configurations"]
                and workflow == rule["workflow"]
                and ds == rule["dates"]
                and scenario in rule["scenarios"]
            ):
                matched.add(ri)
                excluded.append(
                    dict(
                        configuration=label,
                        workflow=workflow,
                        dates=ds,
                        scenario=scenario,
                        status="excluded",
                        reason=rule["reason"],
                        evidence=rule["evidence"],
                    )
                )
                return
        record = workflow == "all_record_local_grid"
        sample = "local_grid" if record else "primary"
        eid = f"{stage}_g{group:02d}_t{trial}_{label}"
        batches = (
            [
                (None, list(range(i, i + cfg["chunk_size"])))
                for i in range(0, len(samples[sample]), cfg["chunk_size"])
            ]
            if record
            else [(d, list(range(len(samples[sample])))) for d in ds]
        )
        jobs = []
        for i, (day, indices) in enumerate(batches):
            jobs.append(
                dict(
                    id=f"{eid}_b{i:02d}",
                    episode=eid,
                    configuration=label,
                    group=group,
                    trial=trial,
                    stage=stage,
                    pair_order=order,
                    position=position,
                    workflow=workflow,
                    scenario=scenario,
                    sample=sample,
                    date=day,
                    batch_position=i + 1,
                    point_indices=indices,
                    points=len(indices),
                    status="pending",
                    phase=(
                        "first_request"
                        if i == 0
                        else "advancing_space" if record else "advancing_day"
                    ),
                    method=(
                        "geographic_point_in_r_batch"
                        if record
                        else (
                            "geographic_point_in_r_dt_batch"
                            if scenario.startswith("radius")
                            else "geographic_nearest_neighbors_batch"
                        )
                    ),
                    fields=list(RECORD_FIELDS if record else WINDOW_FIELDS),
                    temporal_scope="entire_stored_record" if record else "window",
                    half_window_days=cfg["half_window_days"],
                    missions=None if scenario.endswith("all") else cfg["missions"],
                    radius_m=(
                        cfg["radius_m"]
                        if record or scenario.startswith("radius")
                        else None
                    ),
                    cap_seconds=cfg["cap_seconds"],
                    segment="prepared_run",
                )
            )
        episodes.append(
            dict(
                id=eid,
                configuration=label,
                trial=trial,
                group=group,
                workflow=workflow,
                scenario=scenario,
                dates=ds,
                sample=sample,
                stage=stage,
                pair_order=order,
                position=position,
                jobs=jobs,
            )
        )

    rng = random.Random(cfg["schedule_seed"])
    for group in range(len(groups)):
        blocks = ["AA", *(c for c in configs if c not in ("ad_a", "ad_b"))]
        rng.shuffle(blocks)
        for label in blocks:
            if label == "AA":
                order = "AB" if group % 2 == 0 else "BA"
                for pos, c in enumerate(
                    ("ad_a", "ad_b") if order == "AB" else ("ad_b", "ad_a")
                ):
                    add(c, 0, group, "exploration", order, pos)
            else:
                add(label, 0, group, "exploration")
    for trial in range(1, cfg["aa_pairs"]):
        rotation = list(range(len(groups)))
        rng.shuffle(rotation)
        for group in rotation:
            order = "AB" if (trial + group) % 2 == 0 else "BA"
            for pos, c in enumerate(
                ("ad_a", "ad_b") if order == "AB" else ("ad_b", "ad_a")
            ):
                add(c, trial, group, "aa_independent", order, pos)
    if matched != set(range(len(cfg["exclusions"]))):
        raise ValueError(
            "Exclusion does not match this schedule; update explicit dates/workflows"
        )
    jobs = [j for e in episodes for j in e["jobs"]]
    return dict(
        schema_version=1,
        profile=cfg["profile"],
        configurations=configs,
        samples=samples,
        sample_sha256={
            k: hashlib.sha256(json.dumps(v, separators=(",", ":")).encode()).hexdigest()
            for k, v in samples.items()
        },
        episodes=episodes,
        excluded=excluded,
        episode_count=len(episodes),
        batch_count=len(jobs),
        batches_by_workflow=dict(Counter(j["workflow"] for j in jobs)),
    )
