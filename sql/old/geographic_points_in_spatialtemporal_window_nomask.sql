SELECT
	 longitude,
     latitude,
	 sla_filtered,
	 date_time,
	 ST_Distance(ST_GeogFromText('POINT({longitude} {latitude})'),along_track_point) as dist
FROM along_track alt
WHERE ST_DWithin(ST_GeogFromText('POINT({longitude} {latitude})'),along_track_point, {distance})
AND date_time::date BETWEEN {min_date} AND {max_date};