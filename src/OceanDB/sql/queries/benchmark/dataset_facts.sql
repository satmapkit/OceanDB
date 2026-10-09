-- Scan outside timed execution. Counts catch partial ETL; the checksum also
-- detects changes to values when file counts and time ranges stay the same.
SELECT file_name, count(*), min(date_time), max(date_time),
       sum(hashtextextended(t::text, 0)::numeric)::text
FROM public.along_track AS t
GROUP BY file_name ORDER BY file_name;
