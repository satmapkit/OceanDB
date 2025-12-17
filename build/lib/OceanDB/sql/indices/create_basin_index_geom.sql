CREATE INDEX IF NOT EXISTS basin_geog_idx
ON basin USING gist
(basin_geog)
TABLESPACE pg_default;