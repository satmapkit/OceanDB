SELECT
	 longitude,
     latitude,
	 sla_filtered,
	 EXTRACT(EPOCH FROM ({central_date_time} - date_time)) AS time_difference_secs
FROM along_track alt
LEFT JOIN basin on ST_Intersects(basin.basin_geog, alt.along_track_point)
WHERE ST_DWithin(ST_MakePoint(%(longitude)s, %(latitude)s),along_track_point, {distance})
	AND date_time BETWEEN {central_date_time} - {time_delta} AND {central_date_time} + {time_delta}
	AND basin.id IN (
	 	 SELECT DISTINCT
	 	 	 UNNEST(ARRAY[pbc.connected_id, basin_id])
		FROM basin pb
		LEFT JOIN basin_connection pbc on basin_id = pb.id
		WHERE ST_Intersects(pb.basin_geog, ST_MakePoint(%(longitude)s, %(latitude)s))
	);