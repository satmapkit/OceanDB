CREATE INDEX IF NOT EXISTS along_track_point_date_idx
            ON along_track USING gist
            (along_track_point, date_time);