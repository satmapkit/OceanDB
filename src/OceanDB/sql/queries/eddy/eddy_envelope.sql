SELECT {fields}
FROM eddy
LEFT JOIN basin
    ON ST_Intersects(basin.basin_geog, eddy.eddy_point)
LEFT JOIN basin_connections
    ON basin_connections.basin_id = basin.id
WHERE eddy.track = %(track)s AND eddy.cyclonic_type = %(cyclonic_type)s
GROUP BY eddy.track, eddy.cyclonic_type;
