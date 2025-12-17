CREATE INDEX IF NOT EXISTS along_track_basin_idx
    ON public.along_track USING btree
    (basin_id ASC NULLS LAST)
    WITH (deduplicate_items=True);