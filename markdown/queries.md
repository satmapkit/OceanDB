# Queries

OceanDB is designed to support efficient geospatial and temporal queries across ocean satellite datasets.

## Example SLA Query

To query sea level anomaly data for a mission, time range, and point of interest:

```python
latitude = -69
longitude = 28
date = datetime(year=2013, month=3, day=14, hour=5)

data = along_track.geographic_nearest_neighbors_dt(
    latitudes=np.array([latitude]),
    longitudes=np.array([longitude]),
    dates=[date],
    missions=["al"],
)

for d in data:
    print(d)
```

## Query Notes

- Every projected column should be aliased to its schema name.
- Schema objects should not reference query-specific table aliases.

## Related Topics

- See [Indices](indices.md) for index management commands.
- See [Ingest Data](ingest.md) for loading data before querying.
