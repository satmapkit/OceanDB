-- Compare observed values across the two native schemas without requiring equal
-- row IDs, metadata layout, longitude convention, or generated spatial bytes.
-- The legacy tuple importer writes a one-element filename list into a text field.
SELECT trim(both '{}' from file_name) AS source_file, count(*),
       min(date_time), max(date_time),
       sum(hashtextextended(ROW(
           track, cycle, latitude,
           round(longitude::numeric - 360 * floor((longitude::numeric + 180) / 360), 10),
           sla_unfiltered, sla_filtered, date_time, dac, ocean_tide,
           internal_tide, lwe, mdt, basin_id
       )::text, 0)::numeric)::text
FROM public.along_track
GROUP BY source_file ORDER BY source_file;
