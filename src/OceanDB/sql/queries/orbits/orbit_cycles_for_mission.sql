SELECT
	cycle,
    MIN(date_time) AS start_time,
    MAX(date_time) AS end_time
FROM along_track
WHERE split_part(file_name, '_', 3) = %(mission)s
GROUP BY cycle
ORDER BY cycle;