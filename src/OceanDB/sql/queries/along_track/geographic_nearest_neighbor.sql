SELECT
    {fields}
    ST_Distance(
        ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography,
        atk.along_track_point::geography,
        true
    ) AS distance
FROM along_track atk
WHERE date_time BETWEEN %(central_date_time)s - %(time_delta)s::interval
                    AND %(central_date_time)s + %(time_delta)s::interval
  AND basin_id = ANY(%(connected_basin_ids)s)
  AND mission = ANY(%(missions)s)
  AND (
      %(max_radius)s::double precision IS NULL
      OR ST_DWithin(
          ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography,
          atk.along_track_point::geography,
          %(max_radius)s::double precision,
          true
      )
  )
ORDER BY distance, atk.date_time, atk.latitude, atk.longitude
LIMIT 3;
