SELECT
{fields}
FROM eddy
WHERE eddy.track = %(track)s AND eddy.cyclonic_type = %(cyclonic_type)s
ORDER BY observation_number;
