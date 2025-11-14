SELECT
	date_time as time,
	latitude,
	longitude
FROM along_track
WHERE split_part(file_name, '_', 3) = %(mission)s AND cycle = %(cycle)s
ORDER BY date_time;