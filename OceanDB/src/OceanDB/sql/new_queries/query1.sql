SELECT
     latitude,
	 longitude,
	 sla_filtered,
	 EXTRACT(EPOCH FROM (%(central_date_time) - date_time)) AS time_difference_secs
FROM along_track
WHERE ST_DWithin(
    along_track_point::geography,
    ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), $())::geography,
    %(distance)s
)
--AND basin_id = ANY(ARRAY[2, 3, 23, 49, 118, 170, 171, 173, 174, 260, 264, 265, 266])
--AND mission IN ({missions});