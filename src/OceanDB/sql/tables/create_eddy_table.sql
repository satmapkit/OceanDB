CREATE TABLE IF NOT EXISTS eddy
(
<<<<<<< HEAD
    id BIGSERIAL PRIMARY KEY,

    -- Always floats in NetCDF
    amplitude FLOAT4,
    cost_association FLOAT4,

    effective_area FLOAT4,
    effective_contour_height FLOAT4,

    -- Contour lat/lon arrays → representative FLOAT
    effective_contour_latitude FLOAT4,
    effective_contour_longitude FLOAT4,

    effective_contour_shape_error FLOAT4,
    effective_radius FLOAT4,

    inner_contour_height FLOAT4,

    latitude FLOAT4,
    latitude_max FLOAT4,
    longitude FLOAT4,
    longitude_max FLOAT4,

    num_contours INT4,
    num_point_e INT4,
    num_point_s INT4,

    observation_flag BOOLEAN,
    observation_number INT4,

    speed_area FLOAT4,
    speed_average FLOAT4,
    speed_contour_height FLOAT4,

    -- Optional polygon representing contour
    speed_contour_shape GEOGRAPHY,
    speed_contour_shape_error FLOAT4,

    speed_radius FLOAT4,

    date_time TIMESTAMP WITHOUT TIME ZONE,
    track INT4,
    cyclonic_type INT2,

    -- Auto-generated point
    eddy_point GEOGRAPHY(POINT, 4326)
        GENERATED ALWAYS AS (
            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
        ) STORED,

    -- Unique constraints to preserve track
    UNIQUE (track, observation_number, cyclonic_type)
);


--CREATE TABLE IF NOT EXISTS eddy
--(
--    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
--    amplitude int2,
--    cost_association float4,
--    effective_area float4,
--    effective_contour_height float4,
--    effective_contour_latitude int2,
--    effective_contour_longitude int2,
--    effective_contour_shape_error int2,
--    effective_radius int2,
--    inner_contour_height float4,
--    latitude float4,
--    latitude_max float4,
--    longitude float4,
--    longitude_max float4,
--    num_contours int2,
--    num_point_e int2,
--    num_point_s int2,
--    observation_flag boolean,
--    observation_number int2,
--    speed_area float4,
--    speed_average int4,
--    speed_contour_height float4,
--    speed_contour_shape geography,
--    speed_contour_shape_error int2,
--    speed_radius int2,
--    date_time timestamp without time zone,
--    track int,
--    cyclonic_type smallint,
--    eddy_point geography(Point,4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography) STORED,
--    CONSTRAINT eddy_pkey PRIMARY KEY (track, observation_number, cyclonic_type)
--)
=======
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    amplitude int2,
    cost_association float4,
    effective_area float4,
    effective_contour_height float4,
    effective_contour_latitude int2,
    effective_contour_longitude int2,
    effective_contour_shape_error int2,
    effective_radius int2,
    inner_contour_height float4,
    latitude float4,
    latitude_max float4,
    longitude float4,
    longitude_max float4,
    num_contours int2,
    num_point_e int2,
    num_point_s int2,
    observation_flag boolean,
    observation_number int2,
    speed_area float4,
    speed_average int4,
    speed_contour_height float4,
    speed_contour_shape geography,
    speed_contour_shape_error int2,
    speed_radius int2,
    date_time timestamp without time zone,
    track int,
    cyclonic_type smallint,
    eddy_point geography(Point,4326) GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography) STORED,
    CONSTRAINT eddy_pkey PRIMARY KEY (track, observation_number, cyclonic_type)
)
>>>>>>> ingest_hang_fix
