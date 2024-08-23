CREATE INDEX IF NOT EXISTS track_times_cyclonic_type_idx
    ON public.eddy USING btree
    ((track * cyclonic_type) ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;