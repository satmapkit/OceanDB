CREATE INDEX IF NOT EXISTS along_track_file_name_idx
            ON public.{table_name} USING btree
            (file_name COLLATE pg_catalog."default" ASC NULLS LAST)
            WITH (deduplicate_items=True);