import json
import matplotlib.pyplot as plt


filename = "no_mission_singleton_indexes.json"
title = "Compound index without mission + 4 singleton indexes, -60 to 60 lat"
with open(filename, encoding="utf-8") as file:
    nodes = json.load(file)

field_names_short = {
        "mission": "m",
        "basin_id": "b",
        "along_track_point": "p",
        "date_time": "d",
        }

scenario_names = [
        "rdt all missions",
        "NN  all missions",
        "rdt reference missions",
        "NN  reference mission",
        ]
performance = {name: [] for name in scenario_names}
index_names = []
for node in nodes:
    index_name = (
        "" if not node["trial_indexes"] else node["trial_indexes"][0]["name"]
    )
    for field, short_name in field_names_short.items():
        index_name = index_name.replace(field, short_name)
    index_name = index_name.removeprefix("along_track_index_experiment_").replace(
        "_", ""
    )
    index_names.append(index_name)
    print(index_names[-1])

    if node["performance"] is None:
        for name in scenario_names:
            performance[name].append(float('nan'))
        continue
    for scenario_i, res in enumerate(node["performance"]):
        name = scenario_names[scenario_i]
        performance[name].append(res["total_time"])

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
