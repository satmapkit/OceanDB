CREATE INDEX IF NOT EXISTS along_track_point_date_idx
            ON public.{table_name} USING gist
            (along_track_point, date_time);