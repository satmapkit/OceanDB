SELECT
	 longitude,
     latitude,
	 sla_filtered,
	 date_time,
	 ST_DistanceSphere(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326),along_track_point) as dist
FROM along_track alt
LEFT JOIN basin on ST_Contains(basin.basin_geom, alt.along_track_point)
WHERE ST_Contains(ST_Envelope(ST_Buffer(ST_GeogFromText('POINT({longitude} {latitude})'),{distance})::geometry), along_track_point)
	AND date_time::date BETWEEN {min_date} AND {max_date}
	AND basin.id IN (
	 	 SELECT DISTINCT
	 	 	 UNNEST(ARRAY[pbc.connected_id, basin_id])
		FROM basin pb
		LEFT JOIN basin_connection pbc on basin_id = pb.id
		WHERE ST_Contains(pb.basin_geom, ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326))
	);