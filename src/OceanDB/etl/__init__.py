def __getattr__(name):
    if name in {"OceanDBETL", "batch"}:
        from OceanDB.etl.base_etl import OceanDBETL, batch

        return {
            "OceanDBETL": OceanDBETL,
            "batch": batch,
        }[name]
    if name == "BasinsETL":
        from OceanDB.etl.basins_etl import BasinsETL

        return BasinsETL
    if name == "AlongTrackETL":
        from OceanDB.etl.along_track_etl import AlongTrackETL

        return AlongTrackETL
    if name == "EddyETL":
        from OceanDB.etl.eddy_etl import EddyETL

        return EddyETL
    if name == "OceanDBCopernicusMarine":
        from OceanDB.etl.copernicus_marine import OceanDBCopernicusMarine

        return OceanDBCopernicusMarine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
