CREATE TABLE IF NOT EXISTS chelton_eddy
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    amplitude int2,
    cyclonic_type smallint,
    latitude float4,
    longitude float4,
    observation_flag boolean,
    observation_number int2,
    speed_average float4,
    speed_radius int2,
    date_time timestamp without time zone,
    track int,
    chelton_eddy_point geography(Point,4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography) STORED,
    CONSTRAINT chelton_eddy_pkey PRIMARY KEY (track, observation_number, cyclonic_type)
)