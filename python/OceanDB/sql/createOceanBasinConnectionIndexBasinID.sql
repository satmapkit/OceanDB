CREATE INDEX IF NOT EXISTS basin_id_idx
ON public.{table_name} USING btree
(basin_id ASC NULLS LAST, connected_id ASC NULLS LAST)
WITH (deduplicate_items=True)
TABLESPACE pg_default;