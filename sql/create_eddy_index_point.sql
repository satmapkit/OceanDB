CREATE INDEX IF NOT EXISTS eddy_point_idx
            ON public.{table_name} USING gist
            (eddy_point)
            WITH (buffering=auto);