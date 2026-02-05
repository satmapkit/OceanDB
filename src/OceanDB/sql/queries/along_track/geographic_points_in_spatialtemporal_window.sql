SELECT
{fields}
FROM along_track AS atk
WHERE ST_DWithin(
    along_track_point::geography,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography,
    %(distance)s
)
AND date_time BETWEEN %(central_date_time)s - %(time_delta)s::interval
                  AND %(central_date_time)s + %(time_delta)s::interval
AND basin_id = ANY(%(connected_basin_ids)s)
AND mission = ANY(%(missions)s);
