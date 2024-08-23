SELECT
    atk.file_name,
    atk.track,
    atk.cycle,
    atk.latitude,
    atk.longitude,
    atk.sla_unfiltered,
    atk.sla_filtered,
    atk.date_time as time,
    atk.dac,
    atk.ocean_tide,
    atk.internal_tide,
    atk.lwe,
    atk.mdt,
    atk.tpa_correction
FROM eddy
INNER JOIN along_track atk ON atk.date_time BETWEEN eddy.date_time AND (eddy.date_time + interval '1 day')
	AND st_dwithin(atk.along_track_point, eddy.eddy_point, (eddy.speed_radius * {speed_radius_scale_factor} * 2.0)::double precision)
WHERE eddy.track * eddy.cyclonic_type=%(eddy_id)s;