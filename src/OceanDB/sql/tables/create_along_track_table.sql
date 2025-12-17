CREATE TABLE IF NOT EXISTS public.{table_name}
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    file_name text COLLATE pg_catalog."default",
    mission text,
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
    basin_id smallint NOT NULL,
    along_track_point geography(Point,4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography) STORED,
    CONSTRAINT cop_along_pkey PRIMARY KEY (date_time, id),
    -- NO DUPLICATES
    CONSTRAINT along_track_unique_spatiotemporal UNIQUE (date_time, latitude, longitude)
) PARTITION BY RANGE (date_time)