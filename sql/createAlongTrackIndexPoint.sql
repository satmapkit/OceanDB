CREATE INDEX IF NOT EXISTS cat_pt_idx
            ON public.{table_name} USING gist
            (cat_point)
            WITH (buffering=auto);