"""Plot repeated basin+point index timings by query scenario."""

import json
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.patches import Patch


def get_detailed_query_name(performance: dict[str, Any]) -> str:
    query_name = performance["scenario_name"]
    if "AND mission =" in performance["sql"]:
        query_name += " (with mission)"
    else:
        query_name += " (without mission)"
    return query_name


@dataclass
class AvgStats:
    query_name: str
    index_name: str
    planner: str
    scenario_index: int
    total_times: list[float]
    single_query_times: list[float]

    @property
    def mean_total_time(self):
        return mean(self.total_times)

    @property
    def std_total_time(self):
        return stdev(self.total_times) if len(self.total_times) > 1 else 0.0

    @property
    def mean_single_query_times(self):
        return mean(self.single_query_times)

    @property
    def std_single_query_time(self):
        return stdev(self.single_query_times) if len(self.single_query_times) > 1 else 0.0


@dataclass
class ScenarioResults:
    query_name: str
    planner: str
    total_time: float
    single_query_time: float

def extract_scenarios(results: dict[str, Any], trial: dict[str, Any]) -> list[ScenarioResults]:
    scenarios: list[ScenarioResults] = []
    for scenario, performance in zip(results["scenarios"], trial["performance"], strict=True):
        query_name = get_detailed_query_name(performance)
        scenarios.append(ScenarioResults(
            query_name=query_name,
            planner=scenario["planner"],
            total_time=performance["total_time"],
            single_query_time=performance["single_query_sql_time"],
        ))
    return scenarios


def extract_average_stats(results: dict[str, Any]) -> list[AvgStats]:
    stats: dict[tuple[str, int], AvgStats] = {}
    if "results" in results:
        records = (
            (item["database_name"], item["scenario_index"], item["performance"])
            for item in results["results"]
            if item["status"] == "completed" and item["performance"] is not None
        )
        for index_name, scenario_index, performance in records:
            scenario = results["scenarios"][scenario_index]
            case = stats.setdefault(
                (index_name, scenario_index),
                AvgStats(
                    query_name=get_detailed_query_name(performance),
                    index_name=index_name,
                    planner=scenario["planner"],
                    scenario_index=scenario_index,
                    total_times=[],
                    single_query_times=[],
                ),
            )
            case.total_times.append(performance["total_time"])
            case.single_query_times.append(performance["single_query_sql_time"])
        return list(stats.values())

    for trial in results["trials"]:
        index_name = trial["database_name"]
        if trial["performance"] is None:
            continue
        for scenario_index, scenario in enumerate(extract_scenarios(results, trial)):
            case = stats.setdefault((index_name, scenario_index),
                                    AvgStats(
                                        query_name=scenario.query_name,
                                        index_name=index_name,
                                        planner=scenario.planner,
                                        scenario_index=scenario_index,
                                        total_times=[],
                                        single_query_times=[],
                                        )
                                    )
            case.total_times.append(scenario.total_time)
            case.single_query_times.append(scenario.single_query_time)
    return list(stats.values())


def plot(results: dict, output: Path) -> None:
    indexes = [index["database_name"] for index in results["indexes"]]
    if len(indexes) != 2:
        raise ValueError("Expected baseline and basin+point indexes")
    stats = {(s.index_name, s.scenario_index): s for s in extract_average_stats(results)}

    for stat in stats.values():
        print(stat)

    scenario_indices = [
        i for i in range(len(results["scenarios"]))
        if all((index, i) in stats for index in indexes)
    ]
    if not scenario_indices:
        raise ValueError("No scenarios have completed results for every index")
    methods = list(dict.fromkeys(s["method"] for s in results["scenarios"]))
    planners = list(dict.fromkeys(s["planner"] for s in results["scenarios"]))
    colors = {planner: plt.get_cmap("tab10")(i) for i, planner in enumerate(planners)}
    hatches = dict(zip(indexes, ["", "xx"], strict=True))
    legend = ([Patch(facecolor=colors[planner], label=planner) for planner in planners]
              + [Patch(facecolor="white", edgecolor="black", hatch=hatches[index],
                       label=index) for index in indexes])
    fig, axes = plt.subplots(len(methods), 1, figsize=(18, 5 * len(methods)),
                             layout="constrained", squeeze=False)
    for ax, method in zip(axes[:, 0], methods, strict=True):
        positions = [i for i in scenario_indices
                     if results["scenarios"][i]["method"] == method]
        labels = []
        for i in positions:
            scenario = results["scenarios"][i]
            case = stats[indexes[0], i]
            labels.append("\n".join((case.query_name.replace(" (", "\n("), case.planner,
                                     f'jobs={scenario["n_jobs"]}  chunk={scenario["chunk_size"]}')))
        width = 0.26
        for index_offset, index in enumerate(indexes):
            bars = [stats[index, i] for i in positions]
            ax.bar([j + (index_offset - 0.5) * width for j in range(len(positions))],
                   [s.mean_total_time for s in bars],
                   yerr=[s.std_total_time for s in bars], width=width, capsize=3,
                   color=[colors[s.planner] for s in bars], edgecolor="black",
                   hatch=hatches[index])
        ax.set_xticks(range(len(positions)), labels, fontsize=8)
        ax.set_ylabel("Total time (seconds)")
        ax.set_title(method)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(handles=legend, ncol=2)
    fig.suptitle("Baseline vs basin + point index: mean ± sample standard deviation")
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180)
    plt.show()


if __name__ == "__main__":
    input_file = Path("index_benchmark_repeated.json")
    output_file = Path("artifacts/index_performance/basin_point_repeats.png")
    plot(json.loads(input_file.read_text(encoding="utf-8")), output_file)
