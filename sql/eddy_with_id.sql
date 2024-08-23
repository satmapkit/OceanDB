SELECT
	track,
	cyclonic_type,
	date_time,
    ST_Y(eddy_point::geometry) as latitude,
	ST_X(eddy_point::geometry) as longitude,
	observation_number,
	speed_radius,
	amplitude
FROM eddy
WHERE eddy.track=%(track)s AND eddy.cyclonic_type=%(cyclonic_type)s
ORDER BY observation_number;