CREATE INDEX IF NOT EXISTS along_track_date_idx
            ON public.{table_name} USING btree
            ((date_time::date) ASC NULLS LAST)
            WITH (deduplicate_items=True);