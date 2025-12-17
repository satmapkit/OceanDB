SELECT
    longitude,
    latitude,
    sla_filtered,
    EXTRACT(EPOCH FROM (%(central_date_time)s - date_time)) AS time_difference_secs,
    along_track_point <-> ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326) AS distance
FROM along_track
WHERE date_time BETWEEN %(central_date_time)s - %(time_delta)s::interval
                    AND %(central_date_time)s + %(time_delta)s::interval
  AND basin_id = ANY(%(connected_basin_ids)s)
  AND mission = ANY(%(missions)s)
ORDER BY distance
LIMIT 3;