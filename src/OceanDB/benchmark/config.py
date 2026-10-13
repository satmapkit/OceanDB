"""Strict, non-secret benchmark input configuration."""

from copy import deepcopy
from importlib.resources import files
from pathlib import Path

import yaml

DEFAULTS = dict(
    profile="core",
    source_database=None,
    sample_seed=20261013,
    schedule_seed=155353,
    points=512,
    daily_start="2022-10-26",
    daily_days=12,
    interior_date="2022-10-15",
    boundary_date="2022-11-01",
    half_window_days=5,
    missions=["s6a", "j3n"],
    radius_m=50000,
    center=[30.0, -40.0],
    rows=16,
    columns=32,
    spacing_m=25000,
    chunk_size=32,
    aa_pairs=4,
    cap_seconds=30,
    disk_reserve_gib=60,
    exclusions=[],
    exclusion_dataset_sha256=None,
)


def load(path):
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError("Configuration must be a YAML mapping")
    if set(raw) - (set(DEFAULTS) | {"reference_exclusions"}):
        raise ValueError(
            "Unknown configuration keys: "
            + str(sorted(set(raw) - set(DEFAULTS) - {"reference_exclusions"}))
        )
    cfg = deepcopy(DEFAULTS)
    reference = raw.pop("reference_exclusions", False)
    cfg.update(raw)
    if cfg["profile"] not in ("core", "exhaustive"):
        raise ValueError("profile must be core or exhaustive")
    if not isinstance(cfg["source_database"], str) or not cfg["source_database"]:
        raise ValueError("source_database is required")
    for name in ("points", "daily_days", "rows", "columns", "chunk_size", "aa_pairs"):
        if type(cfg[name]) is not int or cfg[name] < 1:
            raise ValueError(name + " must be a positive integer")
    if cfg["aa_pairs"] != 4:
        raise ValueError("This protocol requires exactly four A/A pairs")
    if cfg["cap_seconds"] != 30:
        raise ValueError("This protocol requires a 30-second native batch cap")
    if cfg["rows"] * cfg["columns"] % cfg["chunk_size"]:
        raise ValueError("Grid size must be divisible by chunk_size")
    for name in ("radius_m", "spacing_m", "half_window_days", "disk_reserve_gib"):
        import math

        if (
            not isinstance(cfg[name], (int, float))
            or not math.isfinite(cfg[name])
            or cfg[name] <= 0
        ):
            raise ValueError(name + " must be finite and positive")
    if (
        not isinstance(cfg["missions"], list)
        or not cfg["missions"]
        or not all(isinstance(x, str) for x in cfg["missions"])
    ):
        raise ValueError("missions must be a nonempty string list")
    if reference:
        if cfg["profile"] != "exhaustive":
            raise ValueError("Reference exclusions belong to the exhaustive profile")
        policy = yaml.safe_load(
            files(__package__).joinpath("profiles/exclusions.yaml").read_text()
        )
        cfg["exclusions"] = policy["exclusions"] + cfg["exclusions"]
    if cfg["exclusions"] and not cfg["exclusion_dataset_sha256"]:
        raise ValueError(
            "Bind exclusions to the source inventory SHA256 from benchmark check"
        )
    for rule in cfg["exclusions"]:
        if set(rule) != {
            "configurations",
            "workflow",
            "dates",
            "scenarios",
            "reason",
            "evidence",
        }:
            raise ValueError(
                "Exclusion needs configurations, workflow, dates, scenarios, reason, evidence"
            )
        if {"ad", "ad_a", "ad_b"} & set(rule["configurations"]):
            raise ValueError("AD reference/control cannot be excluded")
    return cfg
