def __getattr__(name):
    if name in {"BaseETL", "batch"}:
        from OceanDB.etl.base_etl import BaseETL, batch

        return {"BaseETL": BaseETL, "batch": batch}[name]
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
