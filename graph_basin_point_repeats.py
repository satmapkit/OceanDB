"""Plot repeated basin+point index timings by query scenario."""

import json
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, stdev
from typing import Any

import matplotlib.pyplot as plt


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
    indexes = [index["database_name"] for index in results["indexes"]
               if len(index["trial_indexes"]) == 1
               and index["trial_indexes"][0]["name"] == "experiment_ba"]
    if len(indexes) != 1:
        raise ValueError("Expected exactly one basin+point index")
    stats = {s.scenario_index: s for s in extract_average_stats(results)
             if s.index_name == indexes[0]}
    if len(stats) != len(results["scenarios"]):
        raise ValueError("No completed basin+point trials for every scenario")
    methods = list(dict.fromkeys(s["method"] for s in results["scenarios"]))
    fig, axes = plt.subplots(len(methods), 1, figsize=(16, 5 * len(methods)),
                             layout="constrained", squeeze=False)
    for ax, method in zip(axes[:, 0], methods, strict=True):
        positions = [i for i, scenario in enumerate(results["scenarios"])
                     if scenario["method"] == method]
        labels = [f'{stats[i].planner} '
                  f'{stats[i].query_name} '
                  f'jobs={results["scenarios"][i]["n_jobs"]} '
                  f'chunk={results["scenarios"][i]["chunk_size"]}'
                  for i in positions]
        ax.bar(range(len(positions)), [stats[i].mean_total_time for i in positions],
               yerr=[stats[i].std_total_time for i in positions], capsize=3)
        ax.set_xticks(range(len(positions)), labels, rotation=60, ha="right")
        ax.set_ylabel("Total time (seconds)")
        ax.set_title(method)
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle("Basin + point index: mean ± sample standard deviation per scenario")
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180)
    plt.show()


if __name__ == "__main__":
    input_file = Path("index_benchmark_repeated.json")
    output_file = Path("artifacts/index_performance/basin_point_repeats.png")
    plot(json.loads(input_file.read_text(encoding="utf-8")), output_file)
