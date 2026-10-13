SELECT t.relname,i.relname,am.amname,pg_get_indexdef(x.indexrelid),
pg_relation_size(i.oid),x.indisvalid,x.indisready,
ARRAY(SELECT pg_get_indexdef(x.indexrelid,k,true) FROM generate_series(1,x.indnkeyatts) k)
FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid
JOIN pg_class t ON t.oid=x.indrelid JOIN pg_am am ON am.oid=i.relam
WHERE t.oid IN (SELECT relid FROM pg_partition_tree('public.along_track'))
ORDER BY t.relname,i.relname;
