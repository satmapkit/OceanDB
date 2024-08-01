SELECT
	 longitude,
     latitude,
	 sla_filtered,
	 date_time,
	 ST_DistanceSphere(ST_SetSRID(ST_MakePoint({longitude}, {latitude}),4326),along_track_point) as dist
FROM along_track alt
WHERE ST_Contains(ST_Envelope(ST_Buffer(ST_GeogFromText('POINT({longitude} {latitude})'),{distance})::geometry), along_track_point)
	AND date_time::date BETWEEN {min_date} AND {max_date};