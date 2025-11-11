CREATE INDEX IF NOT EXISTS chelton_eddy_point_idx
            ON chelton_eddy USING gist
            (chelton_eddy_point)
            WITH (buffering=auto)
            TABLESPACE pg_default;