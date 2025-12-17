CREATE TABLE IF NOT EXISTS basin_connections
(
    pid smallint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 32767 CACHE 1 ),
    connected_id smallint NOT NULL,
    basin_id smallint NOT NULL,
    CONSTRAINT basin_connections_pkey PRIMARY KEY (pid)
)