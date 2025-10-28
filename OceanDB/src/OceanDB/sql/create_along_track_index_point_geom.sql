CREATE INDEX IF NOT EXISTS along_track_point_geom_idx
            ON public.{table_name} USING gist
            ((along_track_point::geometry))
            WITH (buffering=auto);