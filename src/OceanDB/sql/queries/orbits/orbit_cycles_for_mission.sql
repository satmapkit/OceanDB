SELECT
	cycle,
    MIN(date_time) AS start_time,
    MAX(date_time) AS end_time
FROM along_track
WHERE mission = %(mission)s
GROUP BY cycle
ORDER BY cycle;
