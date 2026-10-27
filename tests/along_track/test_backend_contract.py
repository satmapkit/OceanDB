"""Exercise the AlongTrack backend boundary without a PostgreSQL database."""

from collections.abc import Generator
from datetime import datetime, timedelta

import numpy as np
import psycopg
import pytest

from OceanDB.config import Config
from OceanDB.data_access.along_track import (AlongTrack, Mission,
                                             PostgreSQLAlongTrackBackend)
from OceanDB.data_access.base_query import BaseReadQuery
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.schemas.along_track_schema import (along_track_fields,
                                                along_track_schema)

pytestmark = pytest.mark.unit

FIELDS: list[along_track_fields] = ["track", "date_time", "sla_filtered"]
DATE = datetime(2024, 1, 1, 12)
DATES = [DATE, DATE + timedelta(hours=1), DATE + timedelta(hours=2)]
WINDOW = timedelta(hours=6)
LATITUDES = [10.0, 11.0, 12.0]
LONGITUDES = [20.0, 21.0, 22.0]


class _FakeBackend:
    """Minimal serial backend; calls and result objects are observable."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.radius_result: Dataset[along_track_fields] = Dataset(
            name="along_track",
            data={
                "track": np.array([7]),
                "date_time": np.array([DATE], dtype=object),
                "sla_filtered": np.array([900]),
            },
            dtypes={"track": int, "date_time": datetime, "sla_filtered": float},
            schema=along_track_schema,
        )
        self.nearest_result: Dataset[along_track_fields] = Dataset(
            name="along_track",
            data={
                "track": np.array([8]),
                "date_time": np.array([DATE], dtype=object),
                "distance": np.array([1_234.5]),
                "sla_filtered": np.array([900]),
            },
            dtypes={
                "track": int,
                "date_time": datetime,
                "distance": float,
                "sla_filtered": float,
            },
            schema=along_track_schema,
        )

    def geographic_point_in_r_dt(
        self,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        radius: float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
    ) -> Dataset[along_track_fields] | None:
        self.calls.append(
            (
                "radius",
                dict(
                    fields=fields,
                    latitude=latitude,
                    longitude=longitude,
                    date=date,
                    radius=radius,
                    time_window=time_window,
                    missions=missions,
                ),
            )
        )
        return self.radius_result

    def geographic_point_in_r_dt_batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        radius: float = 500_000.0,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        *,
        n_jobs: int = 1,
        chunk_size: int = 16,
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        if len(latitudes) != len(longitudes) or len(latitudes) != len(dates):
            raise ValueError("center coordinates and dates must have equal lengths")
        self.calls.append(
            (
                "radius_batch",
                dict(
                    fields=fields,
                    latitudes=latitudes,
                    longitudes=longitudes,
                    dates=dates,
                    radius=radius,
                    time_window=time_window,
                    missions=missions,
                    n_jobs=n_jobs,
                    chunk_size=chunk_size,
                ),
            )
        )
        results = (self.radius_result, None, self.radius_result)
        for index in range(len(latitudes)):
            yield results[index % len(results)]

    def geographic_nearest_neighbors(
        self,
        fields: list[along_track_fields],
        latitude: float,
        longitude: float,
        date: datetime,
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        *,
        max_radius: float | None = None,
    ) -> Dataset[along_track_fields] | None:
        self.calls.append(
            (
                "nearest",
                dict(
                    fields=fields,
                    latitude=latitude,
                    longitude=longitude,
                    date=date,
                    time_window=time_window,
                    missions=missions,
                    max_radius=max_radius,
                ),
            )
        )
        return self.nearest_result

    def geographic_nearest_neighbors_batch(
        self,
        fields: list[along_track_fields],
        latitudes: list[float],
        longitudes: list[float],
        dates: list[datetime],
        time_window: timedelta = timedelta(days=10),
        missions: list[Mission] | None = None,
        *,
        max_radius: float | None = None,
        n_jobs: int = 1,
        chunk_size: int = 16,
    ) -> Generator[Dataset[along_track_fields] | None, None, None]:
        if len(latitudes) != len(longitudes) or len(latitudes) != len(dates):
            raise ValueError("center coordinates and dates must have equal lengths")
        self.calls.append(
            (
                "nearest_batch",
                dict(
                    fields=fields,
                    latitudes=latitudes,
                    longitudes=longitudes,
                    dates=dates,
                    time_window=time_window,
                    missions=missions,
                    max_radius=max_radius,
                    n_jobs=n_jobs,
                    chunk_size=chunk_size,
                ),
            )
        )
        results = (self.nearest_result, None, self.nearest_result)
        for index in range(len(latitudes)):
            yield results[index % len(results)]


@pytest.fixture
def injected_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[AlongTrack, _FakeBackend]:
    backend = _FakeBackend()

    def fail_postgres_init(
        self: BaseReadQuery, *args: object, **kwargs: object
    ) -> None:
        pytest.fail("An injected backend must not initialize the PostgreSQL reader")

    def fail_connect(*args: object, **kwargs: object) -> None:
        pytest.fail("An injected backend must not connect to PostgreSQL")

    monkeypatch.setattr(BaseReadQuery, "__init__", fail_postgres_init)
    monkeypatch.setattr(psycopg, "connect", fail_connect)
    return AlongTrack(backend=backend), backend


def test_default_backend_still_constructs_postgres_reader() -> None:
    config = Config(postgres_host="unused.example")
    query = AlongTrack(config=config)
    assert isinstance(query.backend, PostgreSQLAlongTrackBackend)
    assert query.postgresql_backend is query.backend
    assert query.backend.config is config
    assert query.backend.connection_string == config.postgres_dsn


def test_backend_owns_its_configuration() -> None:
    with pytest.raises(ValueError, match="config is only used"):
        AlongTrack(config=Config(), backend=_FakeBackend())


def test_injected_backend_forwards_scalar_queries_and_returns_the_same_datasets(
    injected_backend: tuple[AlongTrack, _FakeBackend],
) -> None:
    query, backend = injected_backend
    missions: list[Mission] = ["j2"]

    assert (
        query.geographic_point_in_r_dt(
            FIELDS, 10.0, 20.0, DATE, 12_000.0, WINDOW, missions
        )
        is backend.radius_result
    )
    assert (
        query.geographic_nearest_neighbors(
            FIELDS, 11.0, 21.0, DATE, WINDOW, missions, max_radius=50_000.0
        )
        is backend.nearest_result
    )
    assert backend.nearest_result.row_count == 1
    assert set(backend.radius_result) == set(FIELDS)
    assert set(backend.nearest_result) == {*FIELDS, "distance"}
    assert backend.nearest_result.name == "along_track"
    assert backend.nearest_result["distance"][0] == 1_234.5
    assert backend.nearest_result.get_unscaled("sla_filtered")[0] == 900
    assert backend.nearest_result["sla_filtered"][0] == pytest.approx(0.9)
    assert backend.calls == [
        (
            "radius",
            dict(
                fields=FIELDS,
                latitude=10.0,
                longitude=20.0,
                date=DATE,
                radius=12_000.0,
                time_window=WINDOW,
                missions=missions,
            ),
        ),
        (
            "nearest",
            dict(
                fields=FIELDS,
                latitude=11.0,
                longitude=21.0,
                date=DATE,
                time_window=WINDOW,
                missions=missions,
                max_radius=50_000.0,
            ),
        ),
    ]


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_injected_backend_requires_one_ordered_result_per_center(
    injected_backend: tuple[AlongTrack, _FakeBackend], nearest: bool
) -> None:
    """Keep input order while rejecting malformed batches at call time.

    :param injected_backend: Facade and observable database-free backend.
    :param nearest: Exercise the nearest rather than radius batch.
    """
    query, backend = injected_backend
    method = (
        query.geographic_nearest_neighbors_batch
        if nearest
        else query.geographic_point_in_r_dt_batch
    )
    assert list(method(FIELDS, [], [], [])) == []
    for longitudes, dates in (([], [DATE]), ([20.0], [])):
        with pytest.raises(ValueError, match="equal lengths"):
            method(FIELDS, [10.0], longitudes, dates)
    results = list(method(FIELDS, LATITUDES, LONGITUDES, DATES))
    assert len(results) == len(DATES)
    assert results[1] is None
    assert results[0] is not None and results[0]["date_time"][0] == DATE


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
@pytest.mark.parametrize(
    "n_jobs, chunk_size, message",
    [
        (0, 16, "n_jobs must be at least 1"),
        (1, 0, "chunk_size must be at least 1"),
    ],
)
def test_injected_backend_rejects_invalid_batch_limits(
    injected_backend: tuple[AlongTrack, _FakeBackend],
    nearest: bool,
    n_jobs: int,
    chunk_size: int,
    message: str,
) -> None:
    """Reject worker and chunk limits without consuming a generator.

    :param injected_backend: Facade and observable database-free backend.
    :param nearest: Exercise the nearest rather than radius batch.
    :param n_jobs: Worker count, invalid in the worker-limit case.
    :param chunk_size: Chunk size, invalid in the chunk-limit case.
    :param message: Expected eager validation error.
    """
    query, backend = injected_backend
    method = (
        query.geographic_nearest_neighbors_batch
        if nearest
        else query.geographic_point_in_r_dt_batch
    )
    with pytest.raises(ValueError, match=message):
        method(FIELDS, [], [], [], n_jobs=n_jobs, chunk_size=chunk_size)
    assert backend.calls == []


@pytest.mark.parametrize("radius", [-1.0, float("nan"), float("inf")])
def test_injected_backend_rejects_invalid_nearest_radius(
    injected_backend: tuple[AlongTrack, _FakeBackend], radius: float
) -> None:
    """Validate nearest caps eagerly for both scalar and batch calls.

    :param injected_backend: Facade and observable database-free backend.
    :param radius: Negative or nonfinite cap.
    """
    query, backend = injected_backend
    with pytest.raises(ValueError, match="max_radius must be finite"):
        query.geographic_nearest_neighbors(FIELDS, 10.0, 20.0, DATE, max_radius=radius)
    with pytest.raises(ValueError, match="max_radius must be finite"):
        query.geographic_nearest_neighbors_batch(
            FIELDS, LATITUDES, LONGITUDES, DATES, max_radius=radius
        )
    assert backend.calls == []


def test_injected_backend_preserves_scalar_defaults(
    injected_backend: tuple[AlongTrack, _FakeBackend],
) -> None:
    query, backend = injected_backend
    assert (
        query.geographic_point_in_r_dt(FIELDS, 10.0, 20.0, DATE)
        is backend.radius_result
    )
    assert (
        query.geographic_nearest_neighbors(FIELDS, 10.0, 20.0, DATE)
        is backend.nearest_result
    )
    assert backend.calls == [
        (
            "radius",
            dict(
                fields=FIELDS,
                latitude=10.0,
                longitude=20.0,
                date=DATE,
                radius=500_000.0,
                time_window=timedelta(days=10),
                missions=None,
            ),
        ),
        (
            "nearest",
            dict(
                fields=FIELDS,
                latitude=10.0,
                longitude=20.0,
                date=DATE,
                time_window=timedelta(days=10),
                missions=None,
                max_radius=None,
            ),
        ),
    ]


def test_injected_backend_forwards_batches_in_input_order(
    injected_backend: tuple[AlongTrack, _FakeBackend],
) -> None:
    query, backend = injected_backend
    missions: list[Mission] = ["j2"]

    radius = list(
        query.geographic_point_in_r_dt_batch(
            FIELDS,
            LATITUDES,
            LONGITUDES,
            DATES,
            12_000.0,
            WINDOW,
            missions,
            n_jobs=1,
            chunk_size=1,
        )
    )
    nearest = list(
        query.geographic_nearest_neighbors_batch(
            FIELDS,
            LATITUDES,
            LONGITUDES,
            DATES,
            WINDOW,
            missions,
            max_radius=50_000.0,
            n_jobs=1,
            chunk_size=1,
        )
    )
    assert radius[0] is backend.radius_result and radius[1] is None
    assert radius[2] is backend.radius_result
    assert nearest[0] is backend.nearest_result and nearest[1] is None
    assert nearest[2] is backend.nearest_result
    assert backend.calls == [
        (
            "radius_batch",
            dict(
                fields=FIELDS,
                latitudes=LATITUDES,
                longitudes=LONGITUDES,
                dates=DATES,
                radius=12_000.0,
                time_window=WINDOW,
                missions=missions,
                n_jobs=1,
                chunk_size=1,
            ),
        ),
        (
            "nearest_batch",
            dict(
                fields=FIELDS,
                latitudes=LATITUDES,
                longitudes=LONGITUDES,
                dates=DATES,
                time_window=WINDOW,
                missions=missions,
                max_radius=50_000.0,
                n_jobs=1,
                chunk_size=1,
            ),
        ),
    ]


def test_injected_backend_preserves_batch_defaults(
    injected_backend: tuple[AlongTrack, _FakeBackend],
) -> None:
    query, backend = injected_backend
    assert (
        len(
            list(
                query.geographic_point_in_r_dt_batch(
                    FIELDS, LATITUDES, LONGITUDES, DATES
                )
            )
        )
        == 3
    )
    assert (
        len(
            list(
                query.geographic_nearest_neighbors_batch(
                    FIELDS, LATITUDES, LONGITUDES, DATES
                )
            )
        )
        == 3
    )
    assert backend.calls == [
        (
            "radius_batch",
            dict(
                fields=FIELDS,
                latitudes=LATITUDES,
                longitudes=LONGITUDES,
                dates=DATES,
                radius=500_000.0,
                time_window=timedelta(days=10),
                missions=None,
                n_jobs=1,
                chunk_size=16,
            ),
        ),
        (
            "nearest_batch",
            dict(
                fields=FIELDS,
                latitudes=LATITUDES,
                longitudes=LONGITUDES,
                dates=DATES,
                time_window=timedelta(days=10),
                missions=None,
                max_radius=None,
                n_jobs=1,
                chunk_size=16,
            ),
        ),
    ]


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_injected_backend_does_not_fall_through_to_postgres_streams(
    injected_backend: tuple[AlongTrack, _FakeBackend],
    nearest: bool,
) -> None:
    """Reject unsupported streaming at call time, without SQL fallback.

    :param injected_backend: Facade and observable database-free backend.
    :param nearest: Exercise the nearest rather than radius stream.
    """
    query, backend = injected_backend
    method = (
        query.geographic_nearest_neighbors_stream
        if nearest
        else query.geographic_point_in_r_dt_stream
    )
    with pytest.raises(NotImplementedError):
        method(FIELDS, LATITUDES, LONGITUDES, DATES)
    assert backend.calls == []


@pytest.mark.parametrize("nearest", [False, True], ids=["radius", "nearest"])
def test_injected_backend_rejects_parallel_batches(
    injected_backend: tuple[AlongTrack, _FakeBackend],
    nearest: bool,
) -> None:
    """Reject unsupported parallel batches before generator consumption.

    :param injected_backend: Facade and observable database-free backend.
    :param nearest: Exercise the nearest rather than radius batch.
    """
    query, backend = injected_backend
    method = (
        query.geographic_nearest_neighbors_batch
        if nearest
        else query.geographic_point_in_r_dt_batch
    )
    with pytest.raises(NotImplementedError, match="parallel"):
        method(FIELDS, LATITUDES, LONGITUDES, DATES, n_jobs=2)
    assert backend.calls == []


def test_injected_backend_rejects_postgres_sql_observation(
    injected_backend: tuple[AlongTrack, _FakeBackend],
) -> None:
    query, backend = injected_backend
    with pytest.raises(NotImplementedError, match="SQL observation"):
        query.start_debug(lambda query, params, rendered: None)
    with pytest.raises(NotImplementedError, match="SQL observation"):
        query.stop_debug()
    with pytest.raises(NotImplementedError, match="PostgreSQL database access"):
        query.postgresql_backend
    assert backend.calls == []
