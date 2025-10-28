CREATE INDEX IF NOT EXISTS along_track_point_idx
            ON public.{table_name} USING gist
            (along_track_point)
            WITH (buffering=auto);