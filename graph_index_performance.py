"""Render the index benchmark graph and relative-improvement table."""

import argparse
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt

RESULT_FILES = (Path("index_benchmark.json"),)
OUTPUT_DIRECTORY = Path("artifacts/index_performance")
FIELD_NAMES = {
    "a": "along_track_point",
    "b": "basin_id",
    "d": "date_time",
    "m": "mission",
}
PLOT_STYLES = {
    0: ("Baseline", "*", "#222B35"),
    2: ("Pair", "s", "#AD731D"),
    3: ("Triplet", "o", "#147D92"),
    4: ("Quadruplet", "D", "#9A5CB5"),
}


def total_index_size(node: dict) -> int | None:
    sizes = node.get("index_sizes")
    return sum(sizes.values()) if sizes is not None else None


def index_label(node: dict) -> str:
    if not node["trial_indexes"]:
        return "Baseline"
    names = [
        index["name"].removeprefix("experiment_").removeprefix("exp_")
        for index in node["trial_indexes"]
    ]
    return "+".join(names).upper()


def index_key(node: dict) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (index["table"], index["name"], index["create_sql"])
        for index in node["trial_indexes"]
    )


def scenario_times(node: dict) -> list[float] | None:
    if node.get("status", "completed") != "completed":
        return None
    performance = node.get("performance")
    if not performance:
        return None
    times = [float(result["total_time"]) for result in performance]
    if not all(math.isfinite(time) and time > 0 for time in times):
        return None
    return times


def completed_nodes(nodes: list[dict]) -> tuple[list[dict], dict]:
    baselines = [node for node in nodes if not node["trial_indexes"]]
    if len(baselines) != 1:
        raise ValueError("Expected exactly one baseline")

    baseline = baselines[0]
    baseline_times = scenario_times(baseline)
    if baseline_times is None or total_index_size(baseline) is None:
        raise ValueError("Baseline does not contain complete performance and size data")

    complete = [
        node
        for node in nodes
        if (times := scenario_times(node)) is not None
        and len(times) == len(baseline_times)
        and total_index_size(node) is not None
    ]
    return complete, baseline


def relative_row(node: dict, baseline: dict) -> dict:
    baseline_times = scenario_times(baseline)
    times = scenario_times(node)
    if baseline_times is None or times is None:
        raise ValueError("Incomplete node cannot be summarized")

    ratios = [
        time / baseline_time
        for time, baseline_time in zip(times, baseline_times, strict=True)
    ]
    saved = [100 * (1 - ratio) for ratio in ratios]
    size = total_index_size(node)
    baseline_size = total_index_size(baseline)
    if size is None or baseline_size is None:
        raise ValueError("Missing index size")

    label = index_label(node)
    columns = (
        []
        if label == "Baseline"
        else [
            FIELD_NAMES[letter.lower()]
            for letter in label
            if letter.lower() in FIELD_NAMES
        ]
    )
    return {
        "index": label,
        "index_columns": columns,
        "column_count": len(columns),
        "added_size_mib": (size - baseline_size) / 1024**2,
        "mean_time_saved_percent": sum(saved) / len(saved),
        "geometric_mean_speedup": math.exp(
            -sum(math.log(ratio) for ratio in ratios) / len(ratios)
        ),
        "worst_time_saved_percent": min(saved),
        "total_seconds": sum(times),
    }


def load_results(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload, None
    if payload.get("format_version") != 2 or not isinstance(
        payload.get("comparison"), dict
    ):
        raise ValueError(f"Unsupported results format: {path}")
    comparison = payload["comparison"]
    scenarios = comparison["workload"]["scenarios"]
    nodes = payload["results"]
    for node in nodes:
        performance = node.get("performance") or []
        expected = scenarios[: len(performance)]
        if len(performance) > len(scenarios) or any(
            row.get("scenario_id") != spec["id"]
            or row.get("scenario_name") != spec["method"]
            for row, spec in zip(performance, expected, strict=True)
        ):
            raise ValueError(
                f"Scenario identity/order mismatch in {path}: {index_label(node)}"
            )
        if node["status"] == "completed" and len(performance) != len(scenarios):
            raise ValueError(
                f"Incomplete successful configuration in {path}: {index_label(node)}"
            )
    return nodes, comparison


def compatible_results(result_files):
    loaded = [load_results(path) for path in result_files]
    if not loaded:
        raise ValueError("At least one result file is required")
    if len(loaded) > 1:
        if any(comparison is None for _, comparison in loaded):
            raise ValueError(
                "Cannot verify compatibility of legacy results; plot each file separately"
            )
        if any(comparison != loaded[0][1] for _, comparison in loaded[1:]):
            raise ValueError(
                "Incompatible workloads, datasets, source indexes, code or environments"
            )
    return loaded


def combine_results(result_files: tuple[Path, ...]) -> list[dict]:
    rows_by_index: dict[tuple[tuple[str, str, str], ...], dict] = {}
    for result_file, (nodes, _) in zip(
        result_files, compatible_results(result_files), strict=True
    ):
        complete, baseline = completed_nodes(nodes)
        for node in complete:
            # Each measurement uses its own run's baseline. Later compatible files
            # replace duplicates; record which file supplied each reported result.
            row = relative_row(node, baseline)
            row["source_file"] = str(result_file.resolve())
            rows_by_index[index_key(node)] = row
    return sorted(
        rows_by_index.values(),
        key=lambda row: (-row["mean_time_saved_percent"], row["index"]),
    )


def plot_mean_performance_size_tradeoff(rows: list[dict], output_file: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 7), layout="constrained")
    for column_count, (label, marker, color) in PLOT_STYLES.items():
        points = [row for row in rows if row["column_count"] == column_count]
        if points:
            ax.scatter(
                [row["added_size_mib"] for row in points],
                [row["mean_time_saved_percent"] for row in points],
                color=color,
                label=label,
                marker=marker,
                s=100 if column_count == 0 else 55,
            )

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Index Storage vs Mean Scenario Improvement")
    ax.set_xlabel("Added GiST index storage over baseline (MiB)")
    ax.set_ylabel("Mean scenario time saved versus baseline (%)")
    ax.legend()
    fig.savefig(output_file, dpi=180)
    plt.close(fig)


def write_relative_scenario_improvement(rows: list[dict], output_file: Path) -> None:
    headers = (
        "Rank",
        "Index",
        "Ordered composite columns",
        "Mean time saved",
        "Geometric speedup",
        "Worst scenario",
        "Total seconds",
    )
    body = [
        (
            str(rank),
            row["index"],
            ", ".join(row["index_columns"]) or "None",
            f"{row['mean_time_saved_percent']:+.1f}%",
            f"{row['geometric_mean_speedup']:.3f}x",
            f"{row['worst_time_saved_percent']:+.1f}%",
            f"{row['total_seconds']:.1f}",
        )
        for rank, row in enumerate(rows, 1)
    ]
    table = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
        *["| " + " | ".join(row) + " |" for row in body],
    ]
    output_file.write_text("\n".join(table) + "\n", encoding="utf-8")


def write_status_table(result_files, loaded, output_file):
    lines = [
        "# All configurations",
        "",
        "Only completed configurations have comparable totals. Partial scenarios remain in results.json.",
        "Query totals include result processing and exclude maintenance and EXPLAIN replays. The wall deadline includes EXPLAIN.",
        "",
        "All configurations share the four singleton GiST indexes and source constraints.",
        "",
        "| Run | Index | Ordered composite columns | Status | Completed scenarios | Query total (s) | Failure stage / reason |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for path, (nodes, comparison) in zip(result_files, loaded, strict=True):
        count = len(comparison["workload"]["scenarios"]) if comparison else "?"
        for node in nodes:
            times = scenario_times(node)
            status = node.get(
                "status",
                "completed" if times else "failed (legacy; reason unavailable)",
            )
            total = f"{sum(times):.3f}" if times else "—"
            reason = (
                str(node.get("exception") or "").replace("|", "\\|").replace("\n", " ")
            )
            stage = node.get("failure_stage") or ""
            columns = (
                ", ".join(
                    node.get("fields")
                    or [
                        FIELD_NAMES[c.lower()]
                        for c in index_label(node)
                        if c.lower() in FIELD_NAMES
                    ]
                )
                if node["trial_indexes"]
                else "None"
            )
            lines.append(
                f"| {path.parent.name} | {index_label(node)} | {columns} | {status} | "
                f"{len(node.get('performance') or [])}/{count} | {total} | {stage} {reason} |"
            )
    output_file.write_text("\n".join(lines) + "\n")


def generate_outputs(
    result_files: tuple[Path, ...], output_directory: Path
) -> tuple[Path | None, Path]:
    loaded = compatible_results(result_files)
    output_directory.mkdir(parents=True, exist_ok=True)
    status_file = output_directory / "all_configurations.md"
    write_status_table(result_files, loaded, status_file)
    table_file = output_directory / "relative_scenario_improvement.md"
    graph_file = output_directory / "mean_performance_size_tradeoff.png"
    try:
        rows = combine_results(result_files)
    except ValueError as exc:
        # Relative scores cannot be defined when a run's baseline failed. Keep
        # absolute completed times and every failure visible, and allow packaging.
        reason = str(exc)
        table_file.write_text(
            f"Relative comparison unavailable: {reason}\n\nSee all_configurations.md.\n"
        )
        graph_file.unlink(missing_ok=True)
        (output_directory / "relative_rows.json").unlink(missing_ok=True)
        (output_directory / "analysis-status.json").write_text(
            json.dumps({"status": "unavailable", "reason": reason})
        )
        return None, table_file
    plot_mean_performance_size_tradeoff(rows, graph_file)
    write_relative_scenario_improvement(rows, table_file)
    (output_directory / "relative_rows.json").write_text(
        json.dumps(rows, indent=2, allow_nan=False)
    )
    (output_directory / "analysis-status.json").write_text(
        json.dumps({"status": "completed"})
    )
    return graph_file, table_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", nargs="*", type=Path, default=list(RESULT_FILES))
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIRECTORY)
    arguments = parser.parse_args()
    generate_outputs(tuple(arguments.results), arguments.output_dir)
