SELECT relname, last_analyze, last_autoanalyze, n_live_tup
FROM pg_stat_user_tables
WHERE relname = 'along_track' OR relid IN (
    SELECT inhrelid FROM pg_inherits WHERE inhparent = 'public.along_track'::regclass
)
ORDER BY relname;
