# Indices

OceanDB includes CLI commands for creating, listing, and dropping managed indexes.

## Initialize and Build Indexes

```bash
oceandb init
oceandb index create --all
```

## Create Indexes

Build every OceanDB-managed index with the legacy full-table flow:

```bash
oceandb index create --all
```

Prompt for an along-track index and partition date range:

```bash
oceandb index create
```

## Inspect Indexes

List current indexes:

```bash
oceandb index list
```

## Drop Indexes

Drop all managed indexes:

```bash
oceandb index drop --all
```

Use index commands when you want to control query performance or rebuild indexes after large ingest runs.
