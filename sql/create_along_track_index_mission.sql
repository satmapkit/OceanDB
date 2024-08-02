CREATE INDEX IF NOT EXISTS along_track_mission
    ON public.along_track USING btree
    (split_part(file_name, '_'::text, 5) COLLATE pg_catalog."default" ASC NULLS LAST)