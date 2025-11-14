CREATE INDEX IF NOT EXISTS chelton_track_times_cyclonic_type_idx
    ON chelton_eddy USING btree
    ((track * cyclonic_type) ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;