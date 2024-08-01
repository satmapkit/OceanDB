SELECT
	 longitude,
     latitude,
	 sla_filtered,
	 date_time,
	 ST_DistanceSphere(ST_GeogFromText('POINT({longitude} {latitude})')),along_track_point) as dist
FROM along_track alt
LEFT JOIN basin on ST_Contains(basin.basin_geom, alt.along_track_point)
WHERE ST_DWithin(ST_GeogFromText('POINT({longitude} {latitude})'),along_track_point::geography, 100000)
	AND date_time::date BETWEEN {min_date} AND {max_date}
	AND basin.id IN (
	 	 SELECT DISTINCT
	 	 	 UNNEST(ARRAY[pbc.connected_id, basin_id])
		FROM basin pb
		LEFT JOIN basin_connection pbc on basin_id = pb.id
		WHERE ST_Contains(pb.basin_geog, ST_GeogFromText('POINT({longitude} {latitude})'))
	);