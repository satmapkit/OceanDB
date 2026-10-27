# 03 — Clarify NetCDF decoding as explicit scientific operations

## Status, prerequisites, and navigation

- **Status: proposed; the helpers and new tests described here are not implemented.**
- Prerequisite: [02 — Extract file management](02-extract-file-management.md), accepted with unchanged version-2 cache behavior.
- Retain the independent oracles and baseline from [01 — Characterize behavior and performance](01-characterize-behavior-and-performance.md).
- Read [the series README](README.md), including the common audit and targeted checker protocols.
- Next: [04 — Introduce prepared window](04-introduce-prepared-window.md), which separates window assembly/index ownership rather than changing this decoder.
- This remains behavior-preserving cleanup in steps 01–08; [09 — Persist index snapshots](09-persist-index-snapshots.md) needs separate opt-in approval.

## Goal and current problem

Make the decoding pipeline understandable without tracing a long loop with mixed coordinate, packed-science, and counter policies.
At the series starting point, `src/OceanDB/data_access/indexed_along_track.py::IndexedAlongTrackBackend._times` and `IndexedAlongTrackBackend._prepare_file` contain this logic.
After task 02, their moved equivalents live in the proposed `src/OceanDB/data_access/indexed_files.py`; inspect those actual symbols before editing.
The existing pipeline decodes CF times, reads scaled coordinates, reads raw packed science/counters, builds missing masks, warns on schema metadata mismatch, and validates unique scientific keys.
Those operations have different scaling and missing-data rules and should not look interchangeable.

`src/OceanDB/schemas/along_track_schema.py::along_track_columns_schema` defines packed science scaling and the scalar `tpa_correction` contract.
`src/OceanDB/ocean_data/dataset.py::Dataset.get_unscaled` and `Dataset.__getitem__` distinguish stored packed values from lazily exposed physical values.
`src/OceanDB/ocean_data/ocean_data.py::OceanDataField.apply_scaling` applies the schema scale on access, not during source preparation.
`src/OceanDB/etl/along_track_etl.py::AlongTrackETL.extract_data_from_netcdf` is a comparison point, not a dependency to introduce into the indexed reader.

## Before / after: simplicity, coupling, and readability

| Concern | Before | Proposed after |
| --- | --- | --- |
| Simplicity | One decoder interleaves shape checks, metadata warnings, masking, and assignment. | A short orchestration method names the distinct scientific operations in order. |
| Coupling | Raw NetCDF settings and schema scaling assumptions are implicit across a loop. | Coordinate and packed-column readers state their separate contracts and return aligned arrays. |
| Readability | Readers must infer why some variables auto-scale and others must not. | Intent names and Sphinx docstrings explain physical coordinates versus packed science. |
| Testing | Many malformed-input branches are reachable only through full query fixtures. | Focused temporary-source cases diagnose decoder failures while end-to-end oracles remain authoritative. |

Keep a handful of local functions/methods in `indexed_files.py`; do not invent a generic decoder protocol, shared ETL reader, or transformation registry.
Helpers should remove conceptual branches from the orchestrator, not create one trivial wrapper for every statement.

## Scope and non-goals

- Factor moved `_times`/`_prepare_file` into intent-named time, coordinate, packed-column/mask, science, integer-counter, and unique-key operations.
- Keep NetCDF context/resource lifetime and source-check/cache accounting at the file owner.
- Preserve source dtype layout, cache version/keys, observation order, `source_row`, and provenance consumed by window assembly.
- New tests belong in the existing database-free `tests/along_track/test_indexed_backend.py` unless a focused split is separately justified.
- Do not refactor ETL, schema `ColumnField.from_netcdf`, `Dataset`, shared date utilities, basin lookup, or spatial selection as collateral work.
- Do not broaden validation policy, accepted calendars/shapes, counter range rules, or coordinate support; unsupported north-pole behavior is a separate bugfix.
- Do not pre-scale scientific values, normalize longitudes, round coordinates/times, deduplicate sources, or rewrite cache artifacts.

## Numbered implementation checklist

1. Trace existing NetCDF auto-mask/scale state and write each operation's input/output contract before extracting it.
   Identify which malformed inputs already raise `ValueError` versus lower-level exceptions; preserve existing policy instead of normalizing all failures.
2. Extract an intent-named time decoder, provisionally `decode_time_microseconds`, from moved `_times`.
   Require a complete one-dimensional time array and the existing supported CF calendars: `standard`, `gregorian`, `proleptic_gregorian`.
   Retain `nc.num2date`'s Python-datetime settings and exact integer microseconds relative to the 1950 epoch via the existing UTC-naive conversion.
3. Extract coordinate reading/validation, provisionally `read_coordinates`, with explicitly enabled coordinate auto-scaling.
   Require aligned `(count,)` latitude/longitude arrays, no masked or nonfinite coordinates, and the current latitude bounds `-90 <= latitude < 90`.
   Preserve decoded float64 coordinates exactly; basin lookup may internally wrap longitude, but stored/returned longitude must not be rewritten.
4. Factor a small raw-column operation, provisionally `read_packed_column`, only where it removes duplicated shape/broadcast/mask logic.
   Accept the current scalar or `(count,)` shapes; scalar values and invalidity must broadcast to every observation without losing alignment.
   Disable `set_auto_maskandscale` before raw reads; do not reuse coordinate settings or accidentally read physical science values.
5. Make invalid-mask construction explicit and test it independently through small source fixtures.
   Preserve nonfinite handling, implicit NetCDF default fill when `_FillValue` is absent, explicit `_FillValue`, and `missing_value` comparisons in the packed domain.
   Do not scale sentinels first, turn missing values into zeros, filter whole rows, or add unsupported metadata interpretations opportunistically.
6. Extract packed-science handling, provisionally `read_packed_science`, retaining float64 storage of raw packed values with invalid entries set to `NaN`.
   Keep schema mismatch warnings for source `scale_factor`/`add_offset`; stored values still use OceanDB's packed-value semantics, not source physical units.
   Preserve existing tolerances: scale `rtol=1e-6, atol=0`; zero offset `rtol=0, atol=1e-12`, including float32-compatible scale metadata.
7. Extract integer-counter handling, provisionally `read_integer_counter`, for `track` and `cycle`.
   Preserve rejection of missing/nonfinite/nonintegral values before assigning the current integer columns; retain supported scalar broadcasting.
   Do not silently truncate fractional values or introduce unrelated PostgreSQL smallint range enforcement in this cleanup.
8. Extract a named full-file scientific-key check over `(time_us, latitude, longitude)` after all rows are assembled.
   Retain duplicate rejection across **all** observations, including outside the requested time window; do not make file decoding window-dependent.
   Cross-file uniqueness remains in active-window assembly and must not become a global catalog scan here.
9. Reduce the file decoder to required-variable validation, time/count, coordinates/basins/provenance, column decoding, uniqueness, then source recheck/accounting.
   Preserve required-variable set, error context including path/field, source observation order, file dtype, and `source_row = arange(count)`.
   Advance `source_reads` only after successful decoding and the existing post-read source metadata check; failures must not create valid prepared artifacts.
10. Keep helpers typed as precisely as practical, with local aliases only when they clarify array element/shape roles.
    Validate/narrow NetCDF metadata at external boundaries; do not introduce `typing.cast()`, unchecked `Any` propagation, or error suppressions.
    Use Sphinx/reStructuredText `:param name:`, `:returns:`, and `:raises ExceptionType:` fields for new/updated Python docstrings.
11. Add focused positive and error cases below without replacing literal end-to-end scientific tests.
    Use `tests/data/along_track_reference/generate.py::source_variables` and independent literals as fixture building blocks, not the production decoder as an oracle.
    Create altered sources only in temporary directories; do not regenerate or replace the committed hashed reference asset.
12. Run targeted checks and task 01's phase baseline, emphasizing cold decoding as well as unchanged warm cache hits.
    If extraction exposes a genuine policy bug, document it and propose a separate fix rather than smuggling the change into these commits.

## Invariants

- Required variables remain `time`, `latitude`, `longitude`, `track`, `cycle`, and the existing schema-selected science fields.
- CF time is complete/one-dimensional and represented as UTC-naive integer microseconds; timezone-aware query datetimes remain rejected.
- Source coordinates remain unrounded and unnormalized, preserving sub-microdegree float64 details and stored-longitude tie behavior.
- Scalar corrections broadcast values **and missingness**; every science mask stays aligned with time, coordinates, counters, and provenance.
- Science `_FillValue`/`missing_value`/implicit fill are recognized before scaling. Prepared rows retain packed values; `Dataset` scales exactly once.
- Source scaling mismatch continues to warn while schema scaling governs results; float32 metadata equivalent within current tolerance does not spuriously warn.
- `track`/`cycle` keep current scalar/vector shape acceptance and integral validation; missing counters are errors, not science-style `NaN` values.
- File uniqueness checks all rows; window-level cross-file duplicates remain checked only after inclusive window assembly.
- Cache version **2**, dtype, fingerprint, mmap/LRU behavior, refresh behavior, cumulative counters, and source freshness checks stay unchanged.
- All four queries retain inclusive time/radius, direct-basin-plus-self eligibility including land, signed `delta_t`, optional radius distance, mandatory nearest distance, and legacy empty-field radius results.
- Nearest still returns at most three rows in distance/time/latitude/stored-longitude order; batch timing and partial/empty behavior do not change.

## Proposed atomic commits

1. **Characterize decoder contracts and failures.** Scope: focused temporary-source tests for calendars, shapes, masks, scalar science, and counters.
   Tests: proposed cases below plus existing packed/scalar/full-row oracles. Acceptance: tests describe current accepted inputs and failure policy before helper extraction.
2. **Name time and coordinate operations.** Scope: extract the time/coordinate helpers and keep source layout/accounting untouched.
   Tests: calendar, microsecond, coordinate error, exact-coordinate, and all-four-query cases; targeted checker. Acceptance: exact times/coordinates and unchanged exception behavior.
3. **Name packed science, counter, and unique-key operations.** Scope: raw/broadcast/mask factoring and short file-decoder orchestration.
   Tests: masks/scalars/scaling warnings/counter errors/duplicates, both unit modules, targeted checker, and task 01 baseline. Acceptance: complete packed/physical equivalence and no cache/lifecycle regression.

## Validation commands and proposed test cases

Future implementers run these from the OceanDB root; this document does not report test execution.

```bash
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py tests/along_track/test_backend_contract.py
PYTHONPATH=src:. uv run --no-sync pytest -m unit tests/along_track/test_indexed_backend.py -k 'scalar or fill or scaling or coordinate or calendar or counter or duplicate'
```

The following are **proposed new tests**, not existing symbols:

- `test_decoder_accepts_supported_cf_calendars_with_microseconds`: parameterize the three existing calendars and assert independent midnight-crossing microseconds.
- `test_decoder_rejects_masked_nonvector_time_and_unsupported_calendar`: separate complete-vector and unsupported-calendar failures with contextual assertions.
- `test_decoder_rejects_misaligned_masked_or_nonfinite_coordinates`: build isolated malformed coordinate cases without changing the supported latitude policy.
- `test_scalar_correction_broadcasts_nonmissing_packed_value`: replace the temporary scalar correction with a literal such as `-123`; assert all rows store `-123` and expose `-0.123` exactly once.
- `test_scalar_correction_broadcasts_fill_and_missing_value_masks`: cover scalar fill and scalar `missing_value` independently; assert no neighboring field is masked or dropped.
- `test_decoder_rejects_missing_fractional_or_nonfinite_counters`: parameterize `track`/`cycle` while respecting their source storage types; use floating temporary variables for fractional cases.
- `test_decoder_preserves_scalar_counter_broadcast`: characterize current scalar counter acceptance rather than inadvertently making only corrections scalar-capable.
- `test_decoder_rejects_required_variable_and_column_shape_errors`: cover missing science variables and unsupported multidimensional columns with source/field context.

Retain existing `test_default_netcdf_fill_without_attributes_stays_missing`, float32/mismatched-scaling tests, full source-row/scalar-mask tests, and both within-/cross-file duplicate regressions.
Check warning category/message/context and tolerances; helper extraction may change stack depth, so preserve useful caller context without hard-coding fragile source lines.
Compare all fields and masks against independent literals, plus provenance and required nearest ordering; equal row counts are not equivalence.
Follow the README targeted checker protocol on the file owner, changed adapter imports if any, and modified tests; record unconfigured/unavailable checker limitations.
Rerun the approved phase baseline with the same workload and budgets, not the unrelated `benchmark_runtime.py` experiment.
Never run unrestricted pytest, `make test`, or `make check`: database fixtures may **DROP the configured database**, and check reformats broadly.

## Independent-agent audit checklist

- [ ] A different agent traces actual NetCDF auto-scale/mask settings and every helper's returned units/shapes from source to `Dataset`.
- [ ] Time tests have independent microsecond expectations for supported calendars and do not assert newly broadened calendar policy.
- [ ] Coordinates, packed science, and integer counters remain visibly separate operations with no accidental double scaling.
- [ ] Scalar correction value/mask broadcast, implicit/default fill, explicit sentinels, warning tolerances, and integral counters have focused evidence.
- [ ] Source order/provenance, full-file versus window uniqueness, post-read checks, and successful-read counters stay in the correct lifecycle positions.
- [ ] Tests retain literal scientific oracles and hashed source assets; no shared ETL rewrite, north-pole fix, or unsupported validation hardening slipped in.
- [ ] Full scientific results, cache compatibility, targeted typing, and measured cold/warm performance risks are reported rather than assumed.

### Copyable prompt for a different audit agent

```text
Independently audit task 03 in OceanDB between BASE_REF and HEAD_REF.
Read tasks/README.md's common audit protocol and
tasks/03-clarify-netcdf-decoding.md. Do not edit or commit.
Trace actual moved _times/_prepare_file equivalents and helpers through
Dataset.get_unscaled, Dataset.__getitem__, and schema scaling; do not trust the
implementation narrative. Check CF calendar/UTC-naive microseconds, exact source
coordinates, packed-domain masks, scalar value/mask broadcasting, default/explicit
fills, warning tolerances, integer counters, full-file uniqueness, source rechecks,
and cache/counter lifecycle. Challenge broadened policy, double scaling, lost
alignment, or ETL coupling. Only run explicitly DB-free targeted uv checks;
never unrestricted pytest or make test/check. Return severity-ranked file:symbol
findings, evidence, commands actually run, the README verdict
(accept / changes required / blocked by missing evidence), and untested risks.
Do not implement corrections.
```

## Definition of done

The file decoder reads as an ordered sequence of scientific operations whose units, missingness, shape rules, and error responsibilities are explicit.
Independent tests establish unchanged packed/physical values, scalar masks, exact coordinates/times, cache identity, and lifecycle behavior.
Targeted typing and phase-baseline evidence are recorded without suppressions or unmeasured speed claims, and a different agent has accepted the read-only audit.
