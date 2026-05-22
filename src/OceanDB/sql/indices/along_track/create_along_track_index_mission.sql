CREATE INDEX IF NOT EXISTS along_track_mission_idx
    ON along_track USING btree
    (mission COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (deduplicate_items=True);
