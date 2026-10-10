"""Invoke the unchanged pre-refactor setup and index builder in its own interpreter."""

import builtins
import io
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch


def discovery_missions(files):
    """Use the native importer's filename labels, distinct from SQL mission tokens."""
    labels = set()
    for item in files:
        match = re.fullmatch(r"dt_global_(.+)_phy_l3_.+\.nc", item["name"])
        if not match:
            raise ValueError(f"Unsupported raw filename: {item['name']}")
        labels.add(match[1])
    return sorted(labels)


def run(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    root = Path(manifest["old_root"])
    sys.path.insert(0, str(root / "python"))
    from OceanDB.AlongTrack import AlongTrack

    expected = (root / "python/OceanDB/AlongTrack.py").resolve()
    if Path(sys.modules[AlongTrack.__module__].__file__).resolve() != expected:
        raise RuntimeError("Wrong legacy module imported")
    config_path = (root / "python/config.yaml").resolve()
    config = {
        "db_connect": {
            "host": os.environ["PGHOST"],
            "username": os.environ["PGUSER"],
            "password": os.environ.get("PGPASSWORD", ""),
            "port": int(os.environ["PGPORT"]),
            "db_name": manifest["databases"]["old"],
        }
    }
    original_open = builtins.open

    def open_config(path, *args, **kwargs):
        if isinstance(path, (str, Path)) and Path(path).resolve() == config_path:
            return io.StringIO(json.dumps(config))
        return original_open(path, *args, **kwargs)

    # Supply exactly the selected source files to the native recursive file discovery.
    # Keep these symlinks outside the results directory/ZIP.
    with tempfile.TemporaryDirectory(prefix="oceandb-native-input-") as staging:
        for item in manifest["files"]:
            (Path(staging) / item["name"]).symlink_to(item["path"])
        with patch("builtins.open", side_effect=open_config):
            client = AlongTrack(
                db_name=manifest["databases"]["old"], nc_files_path=staging
            )
        client.create_database()
        client.create_along_track_table()
        client.create_along_track_metadata_table()
        client.create_basin_table()
        client.insert_basin_from_csv()
        client.create_basin_connection_table()
        client.insert_basin_connection_from_csv()
        labels = discovery_missions(manifest["files"])
        if any(label.split("_")[0] not in client.missions for label in labels):
            raise RuntimeError(
                "The native query's all-missions filter would exclude an input mission"
            )
        client.insert_along_track_data_from_netcdf_with_tuples(labels)
        client.create_along_track_indices()  # Includes the original VACUUM ANALYZE.
        result = {"module": str(expected), "basin_map": client.basin_connection_map}
        (directory / "legacy-setup.json").write_text(
            json.dumps(result, indent=2) + "\n"
        )


if __name__ == "__main__":
    run(sys.argv[1])
