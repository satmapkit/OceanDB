CREATE INDEX IF NOT EXISTS basins_geom_idx
ON public.{table_name} USING gist
(basin_geom)
TABLESPACE pg_default;