SELECT
    latitude,
    longitude,
    sla_filtered,
	ST_Distance(ST_MakePoint(%(longitude)s, %(latitude)s),along_track_point) as distance,
    EXTRACT(EPOCH FROM (%(central_date_time)s - date_time)) AS time_difference_secs
FROM along_track
WHERE ST_DWithin(
    along_track_point::geography,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography,
    %(distance)s
)
AND date_time BETWEEN %(central_date_time)s - %(time_delta)s::interval
                  AND %(central_date_time)s + %(time_delta)s::interval
AND basin_id = ANY(%(connected_basin_ids)s)
AND mission = ANY(%(missions)s);
