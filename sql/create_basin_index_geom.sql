CREATE INDEX IF NOT EXISTS basin_geog_idx
ON public.{table_name} USING gist
(basin_geog)
TABLESPACE pg_default;