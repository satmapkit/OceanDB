-- Untimed correctness oracle: include every exact tie at the third neighbor.
SELECT longitude, latitude, sla_filtered,
       EXTRACT(EPOCH FROM (%(central_date_time)s - date_time)) AS delta_t,
       along_track_point <-> ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326) AS distance
FROM along_track
WHERE date_time BETWEEN %(central_date_time)s - %(time_delta)s::interval
                    AND %(central_date_time)s + %(time_delta)s::interval
  AND basin_id = ANY(%(connected_basin_ids)s)
ORDER BY distance
FETCH FIRST 3 ROWS WITH TIES;
