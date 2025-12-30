SELECT
    latitude,
	longitude,
	 sla_filtered,
	 date_time,
	 ST_Distance(ST_MakePoint(%(longitude)s, %(latitude)s),along_track_point) as distance,
	 EXTRACT(EPOCH FROM (%(central_date_time)s - date_time)) AS time_difference_secs
FROM along_track alt
WHERE ST_Within(along_track_point::geometry,ST_MakeEnvelope(%(xmin)s, %(ymin)s,%(xmax)s,%(ymax)s,4326))
	AND date_time BETWEEN %(central_date_time)s - %(time_delta)s AND %(central_date_time)s + %(time_delta)s