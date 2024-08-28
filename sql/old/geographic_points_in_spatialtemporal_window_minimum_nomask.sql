SELECT
	 longitude,
     latitude,
	 sla_filtered,
	 EXTRACT(EPOCH FROM ({central_date_time} - date_time)) AS time_difference_secs
FROM along_track alt
WHERE ST_DWithin(ST_MakePoint(%(longitude)s, %(latitude)s),along_track_point, {distance})
	AND date_time BETWEEN {central_date_time} - {time_delta} AND {central_date_time} + {time_delta}
    AND SPLIT_PART(file_name, '_', 3) IN ({missions});