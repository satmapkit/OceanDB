CREATE INDEX IF NOT EXISTS eddy_point_idx
            ON eddy USING gist
            (eddy_point)
            WITH (buffering=auto)
            TABLESPACE pg_default;