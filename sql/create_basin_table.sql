CREATE TABLE IF NOT EXISTS public.{table_name}
(
    id SERIAL,
    basin_geom geometry(MultiPolygon,4326),
    feature_id integer,
    name character varying(28) COLLATE pg_catalog."default",
    wikidataid character varying(8) COLLATE pg_catalog."default",
    ne_id bigint,
    area double precision,
    CONSTRAINT basins_pkey PRIMARY KEY (id)
)