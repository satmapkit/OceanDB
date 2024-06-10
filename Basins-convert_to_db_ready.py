import pandas as pd

directory = '/Users/briancurtis/Documents/Eddy/ssh_basins/2024_03_08_basin_files/'
loadfile = directory + 'basin_connection_table.txt'

df = pd.read_csv(loadfile, sep=':', 
	engine='python', header=None)
df.set_index(0, inplace= True)
basin_out = []
for key, value in df.iterrows():
	cbasins = value.astype(pd.StringDtype())
	basin_ser = cbasins.str.split(',', expand=True)
	for column in basin_ser.items():
		# print(column[1])
		basinid = int(column[1][1])
		if key != basinid:
			basin_out.append([key, basinid])
basindf = pd.DataFrame(basin_out)
basindf.set_index(0, inplace= True)
basindf.rename(columns={"0": "basinid", "1": "connected_basin"})
basindf.to_csv(directory + 'basin_connections_for_load.csv')
print('Done')