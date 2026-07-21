SELECT {fields} distance
FROM (
    SELECT q.*
    FROM unnest(%(connected_basin_ids)s) AS basins(basin_id)
    CROSS JOIN LATERAL (
        SELECT
            latitude,
            longitude,
            along_track_point <-> ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326) AS distance
        FROM along_track atk
        WHERE date_time BETWEEN %(central_date_time)s - %(time_delta)s::interval
                            AND %(central_date_time)s + %(time_delta)s::interval
          AND mission = ANY(%(missions)s)
          AND basin_id = basins.basin_id
        ORDER BY along_track_point <-> ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)
        LIMIT 3

    ) AS q
) AS atk
ORDER BY distance LIMIT 3;
