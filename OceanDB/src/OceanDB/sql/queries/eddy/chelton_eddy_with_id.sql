SELECT
	track,
	cyclonic_type,
	date_time,
    ST_Y(chelton_eddy_point::geometry) as latitude,
	ST_X(chelton_eddy_point::geometry) as longitude,
	observation_number,
	speed_radius,
	amplitude
FROM chelton_eddy
WHERE track = %(track_cyclonic_type)s
ORDER BY observation_number;