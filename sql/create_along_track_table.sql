CREATE TABLE IF NOT EXISTS public.{table_name}
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    file_name text COLLATE pg_catalog."default",
    track smallint,
    cycle smallint,
    latitude double precision,
    longitude double precision,
    sla_unfiltered smallint,
    sla_filtered smallint,
    date_time timestamp without time zone,
    dac smallint,
    ocean_tide smallint,
    internal_tide smallint,
    lwe smallint,
    mdt smallint,
    tpa_correction smallint,
    along_track_point geometry(Point,4326) GENERATED ALWAYS AS (st_setsrid(st_makepoint(longitude, latitude), 4326)) STORED,
    CONSTRAINT cop_along_pkey PRIMARY KEY (date_time, id)
) PARTITION BY RANGE (date_time)