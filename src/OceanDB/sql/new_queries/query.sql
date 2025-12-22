SELECT
     latitude,
	 longitude,
	 sla_filtered,
	 EXTRACT(EPOCH FROM ({central_date_time} - date_time)) AS time_difference_secs
FROM along_track
WHERE ST_DWithin(
    along_track_point::geography,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography,
    %(distance)s
)
AND date_time BETWEEN %(central_date_time)s - %(time_delta)s
                  AND %(central_date_time)s + %(time_delta)s
AND basin_id = ANY(%(connected_basin_ids)s)
AND mission = ANY(%(missions)s);


--SELECT
--	 longitude,
--     latitude,
--	 sla_filtered,
--	 EXTRACT(EPOCH FROM ({central_date_time} - date_time)) AS time_difference_secs
--FROM along_track
--WHERE ST_DWithin(ST_MakePoint(%(longitude)s, %(latitude)s), along_track_point, %(distance)s)
--AND date_time BETWEEN {central_date_time} - {time_delta} AND {central_date_time} + {time_delta}
--AND basin_id = ANY( %(connected_basin_ids)s )
--AND mission IN ({missions})
--;