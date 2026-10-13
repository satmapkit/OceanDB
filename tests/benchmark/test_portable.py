import copy
import json
from collections import Counter
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pytest
import yaml
from click.testing import CliRunner

from OceanDB.benchmark import schedule, validation
from OceanDB.benchmark.analysis import tables
from OceanDB.benchmark.cache import Cache, TransientCacheState, prepare_until_verified
from OceanDB.benchmark.config import DEFAULTS, load
from OceanDB.benchmark.database import identity
from OceanDB.benchmark.storage import claim, external
from OceanDB.commands.benchmark import benchmark_group

pytestmark = pytest.mark.unit


def test_exhaustive_case_exclusions_preserve_other_scenarios():
    cfg = copy.deepcopy(DEFAULTS)
    cfg["profile"] = "exhaustive"
    policy = Path(schedule.__file__).with_name("profiles") / "exclusions.yaml"
    cfg["exclusions"] = yaml.safe_load(policy.read_text())["exclusions"]
    plan = schedule.build(cfg)
    assert len(plan["configurations"]) == 38
    assert (plan["episode_count"], plan["batch_count"]) == (600, 3812)
    assert len(plan["excluded"]) == 16
    assert sum(r["workflow"] == "daily_sequence" for r in plan["excluded"]) == 4
    for c in ("bma", "mba", "am", "ma", "bm", "mb", "md", "mdb"):
        assert any(
            e["configuration"] == c and e["workflow"] == "all_record_local_grid"
            for e in plan["episodes"]
        )
    assert "mbd" in plan["configurations"] and "bd" in plan["configurations"]
    for cols in plan["configurations"].values():
        assert cols[0] != "date_time"
        if "date_time" in cols and "along_track_point" in cols:
            assert cols.index("along_track_point") < cols.index("date_time")
    pairs = Counter(
        (e["group"], e["trial"])
        for e in plan["episodes"]
        if e["configuration"] == "ad_a"
    )
    assert len(pairs) == 56 and set(pairs.values()) == {1}


def test_policy_requires_dataset_binding_and_exact_dates(tmp_path):
    path = tmp_path / "test.yaml"
    path.write_text(
        "profile: exhaustive\nsource_database: synthetic\nreference_exclusions: true\n"
    )
    with pytest.raises(ValueError, match="Bind"):
        load(path)
    path.write_text(
        path.read_text() + "exclusion_dataset_sha256: synthetic-inventory\n"
    )
    cfg = load(path)
    cfg["boundary_date"] = "2023-11-01"
    with pytest.raises(ValueError, match="does not match"):
        schedule.build(cfg)


def test_preparation_waits_only_for_transient_problems():
    attempts = []
    waits = []

    def attempt(n):
        attempts.append(n)
        if n < 3:
            raise TransientCacheState("busy")
        return "verified"

    assert (
        prepare_until_verified(attempt, lambda _: None, lambda n, r: waits.append(n))
        == "verified"
    )
    assert attempts == [1, 2, 3] and waits == [1, 2]

    def fatal(n):
        raise ValueError("ownership")

    with pytest.raises(ValueError, match="ownership"):
        prepare_until_verified(fatal, lambda _: None, lambda *_: None)


def test_identity_and_file_guard_fail_before_eviction(tmp_path):
    c = Mock()
    c.execute.return_value.fetchone.return_value = (9, "not-owned", "user")
    with pytest.raises(ValueError, match="identity"):
        identity(c, dict(database="x", oid=9, owner_token="ours", owner="user"))
    f = tmp_path / "base/9/123"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"x")
    inv = dict(
        oid=9,
        data_directory=str(tmp_path),
        files=[dict(path=str(f), bytes=1, inode=0, device=0)],
    )
    with pytest.raises(ValueError, match="identity"):
        Cache(tmp_path, {}).pages(inv, True)


def test_claim_is_one_use_and_outputs_reject_checkout(tmp_path):
    p = tmp_path / "go.json"
    claim(p, dict(may_have_started=True))
    with pytest.raises(FileExistsError):
        claim(p, {})
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".git").mkdir()
    with pytest.raises(ValueError, match="outside"):
        external(repo / "ignored/results")
    assert external(tmp_path / "results") == tmp_path / "results"


def test_exact_radius_and_qualified_nn_validation():
    job = dict(
        workflow="isolated",
        scenario="radius_all",
        date="2022-01-01",
        batch_position=1,
        temporal_scope="window",
        half_window_days=5,
    )
    a = np.array([[1, 2, 3, 0]], dtype="<f8")
    b = a.copy()
    b[0, 2] = 4
    rows = [
        dict(job, status="completed", **validation.summarize([x], job)) for x in (a, b)
    ]
    assert validation.validate(rows)["status"] == "failed"
    for r in rows:
        r["scenario"] = "nn_all"
    assert validation.validate(rows)["status"] == "passed_with_qualifications"
    b[0, 0] = 2
    rows[1].update(validation.summarize([b], job))
    assert validation.validate(rows)["status"] == "failed"
    b[0, 3] = 6 * 86400
    with pytest.raises(ValueError, match="temporal"):
        validation.summarize([b], job)


def test_partial_sequence_has_no_total_and_no_control_pair():
    jobs = [
        dict(
            id=str(i),
            episode="e",
            configuration="ad_a",
            workflow="daily_sequence",
            scenario="radius_all",
            date=f"2022-10-{26+i}",
            status=status,
            seconds=1.0,
            trial=0,
            points=2,
            point_indices=[0, 1],
        )
        for i, status in enumerate(["completed", "timed_out", "skipped"])
    ]
    episode = dict(
        id="e",
        configuration="ad_a",
        group=0,
        trial=0,
        workflow="daily_sequence",
        scenario="radius_all",
        jobs=jobs,
    )
    out = tables(dict(jobs=jobs, episodes=[episode]))
    assert out["episodes"][0]["seconds"] is None and not out["aa_episodes"]


def test_cli_interfaces_are_installed():
    result = CliRunner().invoke(benchmark_group, ["--help"])
    assert result.exit_code == 0
    assert all(name in result.output for name in ("check", "prepare", "run", "report"))


def test_supervisor_timeout_keeps_start_marker_and_cannot_repeat(tmp_path, monkeypatch):
    import subprocess
    import sys

    from OceanDB.benchmark import executor
    from OceanDB.benchmark.executor import Worker

    # Real child process, synthetic workload: test worker deadlines without database queries.
    script = """import sys,json,pathlib,time
root=pathlib.Path(sys.argv[1]);pathlib.Path(sys.argv[2]).touch()
for line in sys.stdin:
 j=json.loads(line);p=root/j['directory'];(p/'ready.json').write_text('{}')
 if sys.stdin.readline().strip()!='go':break
 time.sleep(60)
"""
    original = subprocess.Popen

    def start(args, **kwargs):
        return original([sys.executable, "-c", script, args[-2], args[-1]], **kwargs)

    monkeypatch.setattr(executor.subprocess, "Popen", start)
    c = Mock()
    c.execute.return_value.fetchone.return_value = None
    ctx = Mock()
    ctx.__enter__ = Mock(return_value=c)
    ctx.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(executor, "connect", lambda: ctx)
    worker = Worker(
        tmp_path, dict(configurations=dict(ad_a=dict(database="synthetic")))
    )
    job = dict(id="one", configuration="ad_a", scenario="radius_all", cap_seconds=0.05)
    try:
        result = worker.execute(job)
        assert (
            result["status"] == "timed_out" and (tmp_path / "jobs/one/go.json").exists()
        )
        with pytest.raises(FileExistsError):
            worker.execute(job)
    finally:
        worker.close()


def test_report_javascript_synthetic_cases(tmp_path):
    import shutil
    import subprocess

    from OceanDB.benchmark.report import report
    from OceanDB.benchmark.storage import save

    node = shutil.which("node")
    if not node:
        pytest.skip("Node required for report interaction check")
    source = tmp_path / "input"
    source.mkdir()
    job = dict(
        id="one",
        episode="e",
        configuration="ad_a",
        workflow="all_record_local_grid",
        scenario="all_record_radius_all",
        date=None,
        status="completed",
        seconds=1.0,
        trial=0,
        points=1,
        point_indices=[0],
        batch_position=1,
    )
    episode = dict(
        id="e",
        configuration="ad_a",
        group=0,
        trial=0,
        workflow=job["workflow"],
        scenario=job["scenario"],
        jobs=[job],
    )
    save(
        source / "results.json",
        dict(schema_version=1, status="completed", jobs=[job], episodes=[episode]),
    )
    save(
        source / "manifest.json", dict(run_id="synthetic", configurations={"ad_a": {}})
    )
    output = tmp_path / "report"
    report(source, output)
    subprocess.run(
        [
            node,
            str(Path(__file__).with_name("test_report.js")),
            str(output / "summary.html"),
        ],
        check=True,
    )
