SELECT
{fields}
FROM along_track AS atk
WHERE ST_DWithin(
    along_track_point::geography,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geography,
    %(distance)s
)
AND basin_id = ANY(%(connected_basin_ids)s);
