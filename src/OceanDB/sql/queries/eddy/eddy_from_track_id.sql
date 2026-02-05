SELECT
{fields}
FROM eddy
WHERE eddy.track * eddy.cyclonic_type=%(track_id)s
ORDER BY observation_number;