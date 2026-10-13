"""File-only report and verified archive. Never imports code from a saved run."""

import csv
import json
import shutil
import zipfile
from collections import Counter
from importlib.resources import files
from pathlib import Path

from .analysis import tables
from .storage import digest, external, read, save

CAVEATS = [
    "A/A controls describe empirical resolution, median/range/CV and order effects. Four pairs give coarse evidence, not a tolerance gate or precise confidence interval.",
    "Candidate observations are n=1. AD ratios are unpaired descriptive comparisons, not corrected latency, reproducible rankings or finalist selection.",
    "Target user-relation PostgreSQL and OS cache starts are verified. Catalogs, process/library state, other databases and storage-device caches remain. This is not whole-machine cold boot.",
    "Native connection, retrieval and four-field materialization are timed. Client initialization, cache preparation and fingerprints are excluded. No warmups or conditioning.",
    "Later dates/chunks inherit activity within the episode. First chunks already contain spatial reuse; whole episodes must complete to have a total.",
    "Batch timeouts censor the episode; exclusions reflect prior evidence and create selection bias. Neither excluded nor skipped cases have substituted times.",
    "Entire-record queries return every stored observation matching radius, basin and mission predicates. Scientific mean calculation is downstream. Payload bytes are not peak memory; database size is disk storage.",
    "Regional samples and the stored source coverage do not establish full-grid, multi-year, concurrent production, independent rebuild effects or 24 hours of intervening activity.",
    "Warm post-timing diagnostic plans are not plans captured during the native cache-controlled measurements.",
]


def summary(run):
    results = read(run / "results.json")
    if results.get("schema_version") == 1:
        m = read(run / "manifest.json")
        v = (
            read(run / "verification.json")
            if (run / "verification.json").exists()
            else dict(status="unverified")
        )
        return dict(
            run_id=m["run_id"],
            status=results["status"],
            statuses=dict(Counter(j["status"] for j in results["jobs"])),
            accounted=len(results["jobs"]),
            caveats=CAVEATS,
            excluded=results.get("excluded", []),
            verification=v,
            validation=(
                read(run / "validation.json")
                if (run / "validation.json").exists()
                else dict(status="unverified")
            ),
            cache_verification=v.get("cache_verification", []),
            configurations=list(m["configurations"]),
            **tables(results),
        )
    # The accepted combined report is a data schema, not an executable importer.
    s = read(run / "summary.json")
    required = {
        "aa_episodes",
        "aa_dates",
        "aa_chunks",
        "episodes",
        "exploratory_cases",
        "verification",
        "validation",
    }
    if not required <= set(s):
        raise ValueError("Unsupported historical report schema")
    if s["accounted"] != len(results["jobs"]) or s["statuses"] != dict(
        Counter(j["status"] for j in results["jobs"])
    ):
        raise ValueError("Historical summary/status accounting mismatch")
    if len({j["id"] for j in results["jobs"]}) != len(results["jobs"]):
        raise ValueError("Duplicate historical IDs")
    from .legacy import verify

    verify(run, results, s)
    return s


def archive(directory):
    paths = sorted(p for p in directory.rglob("*") if p.is_file())
    hashes = {str(p.relative_to(directory)): digest(p) for p in paths}
    save(directory / "archive-sha256.json", hashes)
    target = directory.with_suffix(".zip")
    if target.exists():
        raise ValueError("Archive already exists")
    with zipfile.ZipFile(
        target, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6
    ) as z:
        for p in [*paths, directory / "archive-sha256.json"]:
            z.write(p, str(p.relative_to(directory)))
    with zipfile.ZipFile(target) as z:
        if z.testzip():
            raise ValueError("ZIP CRC failure")
        import hashlib

        for name, expected in hashes.items():
            with z.open(name) as f:
                h = hashlib.sha256()
                for block in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(block)
                if h.hexdigest() != expected:
                    raise ValueError("ZIP content hash differs: " + name)
    save(
        directory.parent / (directory.name + "-archive-verification.json"),
        dict(status="passed", files=len(hashes), zip_sha256=digest(target)),
    )
    return target


def report(source, output):
    source = Path(source).resolve()
    output = external(output)
    if output.exists() or output.with_suffix(".zip").exists():
        raise ValueError("Use a new external report directory")
    if source == output or source in output.parents:
        raise ValueError("Report must not be nested inside its source run")
    s = summary(source)
    output.mkdir(parents=True)
    # Preserve all evidence without following symlinks out of a run.
    for p in source.rglob("*"):
        if p.is_symlink():
            raise ValueError(
                "Run contains symlink; refusing to archive external content"
            )
    shutil.copytree(source, output / "evidence")
    save(output / "summary.json", s)
    template = files(__package__).joinpath("report.html").read_text()
    (output / "summary.html").write_text(
        template.replace(
            "/*SUMMARY*/null", json.dumps(s, allow_nan=False).replace("<", "\\u003c")
        )
    )
    save(
        output / "calibration.json",
        dict(episodes=s["aa_episodes"], dates=s["aa_dates"], chunks=s["aa_chunks"]),
    )
    for name in ("validation.json", "verification.json", "plans.json"):
        if (source / name).exists():
            shutil.copyfile(source / name, output / name)
    jobs = read(source / "results.json")["jobs"]
    fields = [
        "id",
        "episode",
        "configuration",
        "stage",
        "trial",
        "workflow",
        "scenario",
        "date",
        "batch_position",
        "phase",
        "status",
        "seconds",
        "points",
        "rows",
        "canonical_bytes",
        "pair_order",
        "segment",
        "reason",
    ]
    with (output / "timings.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(jobs)
    return dict(report=str(output / "summary.html"), archive=str(archive(output)))
