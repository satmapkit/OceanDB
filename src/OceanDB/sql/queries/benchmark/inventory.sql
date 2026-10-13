WITH u AS (SELECT oid,reltoastrelid FROM pg_class WHERE relnamespace='public'::regnamespace),
t AS (SELECT reltoastrelid AS oid FROM u WHERE reltoastrelid<>0)
SELECT c.oid,c.relname,pg_relation_filenode(c.oid),pg_relation_filepath(c.oid)
FROM pg_class c WHERE c.relkind IN ('r','m','i','t','S') AND
(c.oid IN (SELECT oid FROM u) OR c.oid IN (SELECT oid FROM t) OR
 c.oid IN (SELECT indexrelid FROM pg_index WHERE indrelid IN (SELECT oid FROM t)))
ORDER BY c.oid;
