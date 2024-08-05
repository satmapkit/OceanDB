SELECT
	 longitude,
     latitude,
	 sla_filtered,
	 date_time,
	 ST_Distance(ST_MakePoint(%(longitude)s, %(latitude)s),along_track_point) as distance,
	 EXTRACT(EPOCH FROM (%(central_date_time)s - date_time)) AS time_difference_secs
FROM along_track alt
LEFT JOIN basin on ST_Intersects(basin.basin_geog, alt.along_track_point)
WHERE ST_Within(along_track_point::geometry,ST_MakeEnvelope(%(xmin)s, %(ymin)s,%(xmax)s,%(ymax)s,4326))
	AND date_time BETWEEN %(central_date_time)s - %(time_delta)s AND %(central_date_time)s + %(time_delta)s
	AND basin.id IN (
	 	 SELECT DISTINCT
	 	 	 UNNEST(ARRAY[pbc.connected_id, basin_id])
		FROM basin pb
		LEFT JOIN basin_connection pbc on basin_id = pb.id
		WHERE ST_Intersects(pb.basin_geog, ST_MakePoint(%(longitude)s, %(latitude)s))
	);