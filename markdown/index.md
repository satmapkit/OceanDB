# Index Management

OceanDB includes CLI commands for creating, listing, showing, and dropping managed indexes.

## Why Indexes Matter

OceanDB queries rely on Postgres indexes for good performance. After ingesting data, you will usually want to build the managed indexes before running heavier spatial or temporal queries.

## Command Summary

OceanDB exposes the following index commands:

```bash
oceandb index create --all
oceandb index create --index-name along_track_index_point
oceandb index create --index-name along_track_index_time --start-date 2024-01-01 --end-date 2024-03-01
oceandb index list
oceandb index show
oceandb index show along_track_index_time
oceandb index show along_track_time_idx
oceandb index summary
oceandb index drop --all
```

## Create All Managed Indexes

Use this when you want OceanDB to build every managed index definition:

```bash
oceandb index create --all
```

This includes along-track, basin, and eddy indexes defined by the project.

## Create One Specific Logical Index

If you want to build just one managed along-track index, start by inspecting its definition:

```bash
oceandb index show along_track_index_point
```

Then create that logical index:

```bash
oceandb index create \
  --index-name along_track_index_point
```

When you run this form, OceanDB will prompt you for:

- `Start partition date (YYYY-MM-DD)`
- `End partition date (YYYY-MM-DD)`

This is useful when you already know which logical index you want, but want to choose the partition window interactively.

## Create One Specific Logical Index Across A Date Range

For along-track indexes, OceanDB can build a single logical index across a selected partition date range.

Example:

```bash
oceandb index create \
  --index-name along_track_index_point \
  --start-date 2024-01-01 \
  --end-date 2024-03-01
```

This command:

- selects the logical index definition `along_track_index_point`
- finds the monthly along-track partitions between the given dates
- creates the partition-specific indexes for those partitions

After creation, you can confirm the partition coverage for that logical index:

```bash
oceandb index summary --index-name along_track_index_point
```

If you omit `--start-date` or `--end-date`, the CLI will prompt you interactively:

```bash
oceandb index create \
  --index-name along_track_index_point
```

The current partition-range flow is available for these along-track logical index names:

- `along_track_index_basin`
- `along_track_index_date`
- `along_track_index_filename`
- `along_track_index_mission`
- `along_track_index_point`
- `along_track_index_point_date`
- `along_track_index_point_date_mission`
- `along_track_index_point_date_mission_basin`
- `along_track_index_point_geom`
- `along_track_index_time`

## Summarize Built Index Coverage

Use `summary` to see:

- the managed indexes that are currently built in Postgres
- the partition coverage for partitioned along-track managed indexes

```bash
oceandb index summary
```

This is especially useful after targeted partition builds, because it shows which partition ranges have already been indexed.

To limit the partition coverage section to one logical along-track index:

```bash
oceandb index summary --index-name along_track_index_time
```

## List Defined And Built Indexes

Use `list` to see both:

- the managed indexes currently built in Postgres
- the managed index definitions OceanDB knows about

```bash
oceandb index list
```

The `BUILT` section shows the managed indexes that are currently present in your Postgres database.

The `DEFINED` section shows:

- the logical OceanDB name
- the table the index targets
- the actual PostgreSQL index name

This is the easiest way to discover what index names are available before using `show` or creating a specific partitioned along-track index.

## Show the CREATE INDEX Statement

Use `show` to print the `CREATE INDEX` SQL for the managed definitions.

Show every managed index definition:

```bash
oceandb index show
```

Show one specific definition by logical name:

```bash
oceandb index show eddy_index_track_cyclonic_type
```

You can also show a definition by the actual PostgreSQL index name:

```bash
oceandb index show track_times_cyclonic_type_idx
```

This is useful when:

- you want to inspect exactly how OceanDB defines an index
- you are comparing query behavior against a specific index
- you want to confirm the table and access method used by a managed index

The command also supports the older option form:

```bash
oceandb index show --index-name eddy_index_track_cyclonic_type
```

## Drop All Managed Indexes

Use this command when you want to remove all OceanDB-managed indexes:

```bash
oceandb index drop --all
```

Add `--yes` to skip the confirmation prompt:

```bash
oceandb index drop --all --yes
```

## Typical Workflow

After ingesting data, a common index workflow is:

```bash
oceandb index list
oceandb index show along_track_index_time
oceandb index create --all
oceandb index summary
```

Or, for a more targeted along-track partition build:

```bash
oceandb index show along_track_index_point
oceandb index create \
  --index-name along_track_index_point
oceandb index create \
  --index-name along_track_index_point \
  --start-date 2024-01-01 \
  --end-date 2024-03-01
oceandb index summary --index-name along_track_index_point
```
