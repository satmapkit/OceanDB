import pickle
import matplotlib.pyplot as plt


filename = "geo_points_in_r_dt.pickle"
with open(filename, "rb") as file:
    data = pickle.load(file)

nodes = data["tried_nodes"]

field_names_short = {
        "mission": "m",
        "basin_id": "b",
        "along_track_point": "p",
        "date_time": "d",
        }

scenario_names = [
        "rdt all",
        "NN  all",
        "rdt ref",
        "NN  ref",
        ]
performance = {name: [] for name in scenario_names}
index_names = []
for node in nodes:
    index_names.append('_'.join(''.join(field_names_short[x] for x in ix.fields)for ix in node.indexes))
    print(index_names[-1])

    for scenario_i, res in enumerate(node.performance):
        name = scenario_names[scenario_i]
        performance[name].append(res.total_time)

fig, ax = plt.subplots(layout='constrained')

res = ax.grouped_bar(performance, tick_labels=index_names, group_spacing=1)
for container in res.bar_containers:
    ax.bar_label(container, padding=3)

# Add some text for labels, title, etc.
ax.set_ylabel('Time')
ax.legend(loc='upper left')

plt.show()
