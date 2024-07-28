CREATE TABLE IF NOT EXISTS public.{table_name}
(
    idx bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    nme text COLLATE pg_catalog."default",
    track smallint,
    cycle smallint,
    lat double precision,
    lon double precision,
    sla_unfiltered smallint,
    sla_filtered smallint,
    date_time timestamp without time zone,
    dac smallint,
    ocean_tide smallint,
    internal_tide smallint,
    lwe smallint,
    mdt smallint,
    tpa_correction smallint,
    cat_point geometry(Point,4326) GENERATED ALWAYS AS (st_setsrid(st_makepoint(lon, lat), 4326)) STORED,
    CONSTRAINT cop_along_pkey PRIMARY KEY (date_time, idx)
) PARTITION BY RANGE (date_time)