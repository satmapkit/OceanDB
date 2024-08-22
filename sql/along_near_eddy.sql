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
INNER JOIN along_track atk ON atk.date_time::date = eddy.date_time::date AND st_dwithin(atk.along_track_point, eddy.eddy_point, (eddy.speed_radius * 2.0)::double precision)
where eddy.track=%(track)s AND eddy.cyclonic_type=%(cyclonic_type)s;