SELECT
	date_time as time,
	latitude,
	longitude
FROM along_track
WHERE mission = %(mission)s AND cycle = %(cycle)s
ORDER BY date_time;
