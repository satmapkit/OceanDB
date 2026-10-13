"""Durable run files; results never belong in a source checkout."""

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w") as f:
        json.dump(value, f, indent=2, allow_nan=False, default=str)
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


def claim(path, value):
    """Atomic one-use marker. Existing/ambiguous starts are never resumed."""
    with Path(path).open("x") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())


def now():
    return datetime.now(timezone.utc).isoformat()


def event(run, name, **details):
    record = dict(time=now(), event=name, **details)
    line = json.dumps(record, default=str)
    with (Path(run) / "progress.log").open("a") as f:
        f.write(line + "\n")
    print(line, flush=True)


def external(path):
    path = Path(path).expanduser().resolve()
    for parent in (path, *path.parents):
        if (parent / ".git").exists():
            raise ValueError("Benchmark output must be outside every Git checkout")
    return path


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def package_hashes():
    root = Path(__file__).resolve().parents[1]
    return {
        str(p.relative_to(root)): digest(p)
        for p in sorted(root.rglob("*"))
        if p.is_file()
        and p.suffix in (".py", ".sql", ".c", ".yaml", ".html", ".csv", ".nc")
    }


def verify_code(run):
    if read(Path(run) / "code-sha256.json") != package_hashes():
        raise ValueError("Installed code/resources changed after preparation")
