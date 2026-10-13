import math
from collections import Counter
from copy import deepcopy
from datetime import datetime
from unittest.mock import Mock

import numpy as np
import pytest

from OceanDB.benchmark import workloads as core
from OceanDB.benchmark.config import DEFAULTS
from OceanDB.benchmark.schedule import build

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def plan():
    return build(deepcopy(DEFAULTS))


def test_grid_is_deterministic_overlapping_and_contiguous_at_row_turns():
    points = core.local_grid()
    assert points == core.local_grid() and len({tuple(p) for p in points}) == 512
    # Independent great-circle formula, including transitions between rows.
    for a, b in zip(points, points[1:]):
        la, lo, lb, lob = map(math.radians, (*a, *b))
        hav = (
            math.sin((lb - la) / 2) ** 2
            + math.cos(la) * math.cos(lb) * math.sin((lob - lo) / 2) ** 2
        )
        distance = 2 * core.EARTH_RADIUS_M * math.asin(math.sqrt(hav))
        assert 24_900 < distance < 25_100
        assert distance < core.RADIUS_M
    wrapped = core.local_grid(center=(30, 179))
    assert all(-180 <= p[1] < 180 for p in wrapped)
    assert any(p[1] < 0 for p in wrapped)


def test_schedule_retains_old_core_and_adds_new_workload_without_singletons(plan):
    assert plan["episode_count"] == 196 and plan["batch_count"] == 1232
    assert "baseline" not in plan["configurations"]
    jobs = [j for e in plan["episodes"] for j in e["jobs"]]
    assert len({j["id"] for j in jobs}) == len(jobs)
    assert sum(j["workflow"] != "all_record_local_grid" for j in jobs) == 784
    assert sum(j["workflow"] == "all_record_local_grid" for j in jobs) == 448
    for e in plan["episodes"]:
        js = e["jobs"]
        assert [j["batch_position"] for j in js] == list(range(1, len(js) + 1))
        assert all(j["cap_seconds"] == 30 for j in js)
        if e["workflow"] == "all_record_local_grid":
            assert len(js) == 16 and [
                i for j in js for i in j["point_indices"]
            ] == list(range(512))
            assert all(
                j["points"] == 32
                and j["date"] is None
                and j["fields"] == list(core.RECORD_FIELDS)
                for j in js
            )
        elif e["workflow"] == "daily_sequence":
            assert [j["date"] for j in js] == list(core.DATES)
            assert all(j["points"] == 512 for j in js)


def test_every_control_group_has_four_adjacent_balanced_pairs(plan):
    episodes = plan["episodes"]
    for group in range(14):
        pairs = []
        for trial in range(4):
            indices = [
                i
                for i, e in enumerate(episodes)
                if e["group"] == group
                and e["trial"] == trial
                and e["configuration"] in ["ad_a", "ad_b"]
            ]
            assert len(indices) == 2 and indices[1] == indices[0] + 1
            first, second = [episodes[i] for i in indices]
            assert {first["configuration"], second["configuration"]} == {"ad_a", "ad_b"}
            pairs.append(first["pair_order"])
        assert Counter(pairs) == {"AB": 2, "BA": 2}
        for label in ["amd", "bad", "abd", "bma", "bmad", "mba"]:
            assert (
                len(
                    [
                        e
                        for e in episodes
                        if e["group"] == group and e["configuration"] == label
                    ]
                )
                == 1
            )


def test_native_adapter_preserves_local_order_and_has_no_date_or_aggregation(plan):
    ep = next(e for e in plan["episodes"] if e["workflow"] == "all_record_local_grid")
    job = ep["jobs"][1]
    obj = Mock()
    obj.geographic_point_in_r_batch.return_value = iter([None] * 32)
    assert core.execute_batch(obj, job, plan["samples"]) == [None] * 32
    kwargs = obj.geographic_point_in_r_batch.call_args.kwargs
    assert "dates" not in kwargs and "time_window" not in kwargs
    assert kwargs["radius"] == 50_000 and kwargs["fields"] == list(core.RECORD_FIELDS)
    assert kwargs["latitudes"] == [p[0] for p in plan["samples"]["local_grid"][32:64]]


def test_adapter_retains_old_window_and_unbounded_nn_semantics(plan):
    for method in [
        "geographic_point_in_r_dt_batch",
        "geographic_nearest_neighbors_batch",
    ]:
        job = next(
            j for e in plan["episodes"] for j in e["jobs"] if j["method"] == method
        )
        obj = Mock()
        getattr(obj, method).return_value = iter([None] * 512)
        core.execute_batch(obj, job, plan["samples"])
        kwargs = getattr(obj, method).call_args.kwargs
        assert kwargs["time_window"].days == 5 and len(kwargs["dates"]) == 512
        if method.endswith("neighbors_batch"):
            assert kwargs["max_radius"] is None


def test_record_fingerprint_is_order_invariant_and_detects_oldest_and_newest_rows():
    first = (
        [1.0, 2.0],
        [3.0, 4.0],
        [0.1, 0.2],
        [datetime(1993, 1, 1), datetime(2022, 11, 30)],
    )
    reversed_rows = tuple(list(reversed(column)) for column in first)
    a = core.summarize_record([first, None])
    b = core.summarize_record([reversed_rows, None])
    assert (
        a == b
        and a["counts"] == [2, 0]
        and a["returned_months"] == {"1993-01": 1, "2022-11": 1}
    )
    changed = (*first[:3], [datetime(1993, 1, 1), datetime(2022, 11, 30, 0, 0, 0, 1)])
    assert a["sha256"] != core.summarize_record([changed, None])["sha256"]
    assert (
        a["sha256"]
        != core.summarize_record([tuple(c[:1] for c in first), None])["sha256"]
    )
    assert a["sha256"] != core.summarize_record([None, first])["sha256"]


def test_record_fingerprint_normalizes_nan_and_rejects_missing_timestamps():
    rows = ([-0.0], [0.0], [float("nan")], [datetime(2022, 10, 1)])
    normalized = ([0.0], [-0.0], [np.nan], [datetime(2022, 10, 1)])
    assert (
        core.canonical_record(rows).tobytes()
        == core.canonical_record(normalized).tobytes()
    )
    with pytest.raises(ValueError, match="date_time"):
        core.canonical_record(([0.0], [0.0], [0.1], [None]))
