CREATE INDEX IF NOT EXISTS along_track_point_date_idx
            ON public.{table_name} USING gist
            (cat_point, (date_time::date))
            WITH (buffering=auto);