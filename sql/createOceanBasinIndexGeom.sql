CREATE INDEX IF NOT EXISTS sidx_basins_geom
ON public.{table_name} USING gist
(geom)
TABLESPACE pg_default;