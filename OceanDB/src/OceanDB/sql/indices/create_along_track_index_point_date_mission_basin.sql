CREATE INDEX IF NOT EXISTS along_track_point_date_mission_basin_idx
    ON along_track USING gist
    (along_track_point, date_time, basin_id, split_part(file_name, '_'::text, 3) COLLATE pg_catalog."default")
    WITH (buffering=auto);