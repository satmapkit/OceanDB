##  Projection-Aware Dataset Architecture

A schema-contract–driven data access layer that:
	•	defines data once (schema contracts),
	•	uses those contracts to:
	•	build SQL queries,
	•	validate query results,
	•	construct immutable, typed datasets,
	•	and exposes domain-specific dataset views for scientific computation.

## Design

Primary goals
	1.	Correctness across systems
	•	SQL ↔ Python ↔ NumPy ↔ NetCDF agree on meaning and type
	2.	Explicit semantics
	•	A query result means something, not just “some rows”
	3.	Projection safety
	•	You can’t accidentally return malformed data
	4.	Reusability
	•	Same schema powers ingestion, querying, and export
	5.	Domain clarity
	•	Along-track data behaves like along-track data, not generic tables