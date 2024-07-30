CREATE INDEX IF NOT EXISTS nme_alng_idx
            ON public.{table_name} USING btree
            (nme COLLATE pg_catalog."default" ASC NULLS LAST)
            WITH (deduplicate_items=True);