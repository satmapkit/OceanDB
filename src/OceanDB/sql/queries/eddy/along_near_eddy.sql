SELECT {fields}
FROM eddy
INNER JOIN along_track atk
    ON atk.date_time BETWEEN eddy.date_time AND (eddy.date_time + interval '1 day')
   AND ST_DWithin(
        atk.along_track_point,
        eddy.eddy_point,
        (CAST(eddy.speed_radius AS double precision) * %(speed_radius_scale_factor)s * 2.0)
   )
WHERE eddy.track * eddy.cyclonic_type = %(track_id)s
  AND atk.date_time BETWEEN %(min_date)s AND %(max_date)s
  AND atk.basin_id = ANY(%(basin_ids)s::int[]);
