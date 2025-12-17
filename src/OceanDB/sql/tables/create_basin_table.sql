CREATE TABLE IF NOT EXISTS basin
(
    id SERIAL,
    basin_geog geography(MultiPolygon),
    feature_id integer,
    name character varying(28) COLLATE pg_catalog."default",
    wikidataid character varying(8) COLLATE pg_catalog."default",
    ne_id bigint,
    area double precision,
    CONSTRAINT basins_pkey PRIMARY KEY (id)
)