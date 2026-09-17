import csv
import json
from pathlib import Path

import matplotlib.pyplot as plt


filename = "no_mission_singleton_indexes.json"
title = "Compound index without mission + 4 singleton indexes, -60 to 60 lat"
with open(filename, encoding="utf-8") as file:
    nodes = json.load(file)

scenario_names = [
        "rdt all missions",
        "NN  all missions",
        "rdt reference missions",
        "NN  reference mission",
        ]
performance = {name: [] for name in scenario_names}
index_names = []
for node in nodes:
    if node["trial_indexes"]:
        index_name = "_".join(index["name"] for index in node["trial_indexes"])
        index_name = index_name.replace("exp_", "")
    else:
        index_name = "baseline"
    index_names.append(index_name)

    if node["performance"] is None:
        for name in scenario_names:
            performance[name].append(float('nan'))
        continue
    for scenario_i, res in enumerate(node["performance"]):
        name = scenario_names[scenario_i]
        performance[name].append(res["total_time"])


def total_index_size(node):
    index_sizes = node["index_sizes"]
    return sum(index_sizes.values()) if index_sizes is not None else None


def export_summary(nodes, index_names, output_file):
    baseline = next(node for node in nodes if not node["trial_indexes"])
    baseline_size = total_index_size(baseline)
    fieldnames = [
        "index",
        "trial_indexes",
        "status",
        "total_runtime_seconds",
        "total_index_size_bytes",
        "added_index_size_bytes",
        *[f"{scenario} runtime seconds" for scenario in scenario_names],
    ]

    with output_file.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for node, index_name in zip(nodes, index_names, strict=True):
            node_performance = node["performance"]
            size = total_index_size(node)
            row = {
                "index": index_name or "Baseline",
                "trial_indexes": ";".join(
                    index["name"] for index in node["trial_indexes"]
                ),
                "status": "ok" if node_performance is not None else "failed",
                "total_runtime_seconds": node["error"],
                "total_index_size_bytes": size,
                "added_index_size_bytes": (
                    size - baseline_size
                    if size is not None and baseline_size is not None
                    else None
                ),
            }
            if node_performance is not None:
                row.update(
                    {
                        f"{scenario} runtime seconds": result["total_time"]
                        for scenario, result in zip(
                            scenario_names, node_performance, strict=True
                        )
                    }
                )
            writer.writerow(row)


def plot_performance_size_tradeoff(nodes, index_names, output_file):
    baseline = next(node for node in nodes if not node["trial_indexes"])
    baseline_runtime = baseline["error"]
    baseline_size = total_index_size(baseline)
    if baseline_runtime is None or baseline_runtime <= 0 or baseline_size is None:
        raise ValueError("Baseline trial does not contain runtime and index size data")

    plotted_nodes = [
        (node, index_name, size)
        for node, index_name in zip(nodes, index_names, strict=True)
        if node["error"] is not None
        if (size := total_index_size(node)) is not None
    ]
    added_size_mib = [
        (size - baseline_size) / (1024**2) for _, _, size in plotted_nodes
    ]
    improvement_percent = [
        100 * (baseline_runtime - node["error"]) / baseline_runtime
        for node, _, _ in plotted_nodes
    ]

    fig, ax = plt.subplots(figsize=(10, 7), layout="constrained")
    ax.scatter(added_size_mib, improvement_percent, s=70, color="#176B87")
    for (_, index_name, _), size, improvement in zip(
        plotted_nodes, added_size_mib, improvement_percent, strict=True
    ):
        ax.annotate(
            index_name or "Baseline",
            (size, improvement),
            xytext=(5, 5),
            textcoords="offset points",
        )

    ax.axhline(0, color="black", linewidth=0.8)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_title("Runtime Improvement Versus Added Index Size")
    ax.set_xlabel("Index size added over baseline (MiB)")
    ax.set_ylabel("Total runtime improvement over baseline (%)")
    ax.grid(alpha=0.25)
    fig.savefig(output_file, dpi=180)


output_directory = Path("artifacts/index_performance")
output_directory.mkdir(parents=True, exist_ok=True)
export_summary(nodes, index_names, output_directory / "summary.csv")
plot_performance_size_tradeoff(
    nodes,
    index_names,
    output_directory / "performance_size_tradeoff.png",
)

fig, ax = plt.subplots(layout='constrained')

res = ax.grouped_bar(performance, tick_labels=index_names, group_spacing=1)
for container in res.bar_containers:
    ax.bar_label(container, padding=5, fmt='%.0f', label_type="edge", rotation='vertical')
    # ax.bar_label(rects1, padding=5, fmt='%.2f', label_type='edge', fontsize=9, rotation='vertical')

max_time = max(max(group) for group in performance.values())
ax.set_ylim((0, max_time*1.3))

# Add some text for labels, title, etc.
ax.set_ylabel('Time (s)')
ax.legend(loc='upper left')
ax.set_title(title)

plt.show()
