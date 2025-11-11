CREATE INDEX IF NOT EXISTS along_track_mission_idx
    ON along_track USING btree
    (split_part(file_name, '_'::text, 3) COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (deduplicate_items=True);