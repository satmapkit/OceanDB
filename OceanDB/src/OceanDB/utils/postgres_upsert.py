from typing import List, Tuple, Any, Iterable

from sqlalchemy.engine import Connection
from sqlalchemy.sql.schema import Table
from sqlalchemy.dialects.postgresql import insert


def upsert_ignore(
        table: Table,
        conn: Connection,
        keys: List[str],
        data_iter: Iterable[Tuple[Any, ...]],
) -> None:
    """
    In practice it should never happen but in development it might be the case that we attempt to insert duplicate rows.
    Added a constraint on the along_track table
    CONSTRAINT along_track_unique_spatiotemporal UNIQUE (date_time, latitude, longitude)

    Custom pandas.to_sql 'method' that performs bulk INSERTs into PostgreSQL
    and ignores duplicates based on a unique constraint.

    Parameters
    ----------
    table : sqlalchemy.sql.schema.Table
        SQLAlchemy Table object being written to by pandas.to_sql().
    conn : sqlalchemy.engine.Connection
        Active SQLAlchemy connection or transaction context.
    keys : list of str
        Column names (in order) corresponding to DataFrame columns.
    data_iter : iterable of tuple
        Iterable of row value tuples to insert.

    Returns
    -------
    None
        Executes an INSERT ... ON CONFLICT DO NOTHING for each chunk.
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=['date_time', 'latitude', 'longitude']  # your unique columns
    )
    conn.execute(stmt)