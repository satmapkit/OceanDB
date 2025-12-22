from OceanDB.OceanDB import OceanDB
import psycopg as pg


class Eddy(OceanDB):
    eddy_table_name: str = 'eddy'
    eddy_metadata_table_name: str = 'eddy_metadata'
    eddies_file_path: str
    eddy_variable_metadata: dict = dict()
    variable_scale_factor: dict = dict()
    variable_add_offset: dict = dict()

    along_track_near_eddy_query = 'queries/eddy/along_track_near_eddy.sql'

    def __init__(self):
        super().__init__()

    def along_track_points_near_eddy(self, eddy_id):
        values = {"eddy_id": eddy_id}

        query = self.load_sql_file(self.along_track_near_eddy_query)

        with pg.connect(self.config.postgres_dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                data = cursor.fetchall()
                # values["min_date"] = data[0][0]
                # values["max_date"] = data[0][1]
                along_query = query.format(speed_radius_scale_factor=self.variable_scale_factor["speed_radius"],
                                                 min_date=data[0][0],
                                                 max_date=data[0][1],
                                                 connected_basin_ids=data[0][2])
                cursor.execute(along_query, values)
                data = cursor.fetchall()

        return data

