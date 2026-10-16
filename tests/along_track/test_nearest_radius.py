"""Validate nearest-neighbor radius options without a database."""

import re
from collections.abc import Callable, Generator, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from inspect import Parameter, signature
from typing import Literal, NoReturn, TypedDict

import pytest

from OceanDB.data_access.along_track import AlongTrack, Mission
from OceanDB.data_access.base_query import QueryPlan
from OceanDB.ocean_data.dataset import Dataset
from OceanDB.query_spec import QuerySpec
from OceanDB.schemas.along_track_schema import along_track_fields

pytestmark = pytest.mark.unit
Surface = Literal["scalar", "batch", "stream"]
DATE = datetime(2013, 1, 4, 12)
WINDOW = timedelta(hours=2)


class _RadiusOptions(TypedDict, total=False):
    max_radius: float | None


class _BasinMask:
    def lookup(self, latitude: float, longitude: float) -> int:
        return 1


class _BasinConnections:
    connection_map: dict[int, list[int]] = {1: [1, 2]}


@dataclass
class _Execution:
    query_spec: QuerySpec[along_track_fields]
    params: list[dict[str, object]]
    plan: QueryPlan


@pytest.fixture
def captured_query(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[AlongTrack, list[_Execution]]:
    query = AlongTrack.__new__(AlongTrack)
    query.__dict__["basin_mask_lookup"] = _BasinMask()
    query.__dict__["basin_connections"] = _BasinConnections()
    executions: list[_Execution] = []

    def fake_execute(
        query_spec: QuerySpec[along_track_fields],
        *,
        fields: Iterable[along_track_fields],
        params_batch: Iterable[Mapping[str, object]],
        plan: QueryPlan,
        dataset_name: str = "query_result",
    ) -> Generator[tuple[int, Dataset[along_track_fields] | None], None, None]:
        params = [dict(values) for values in params_batch]
        plan.validate(len(params))
        assert tuple(fields) == ("track",)
        assert dataset_name == "along_track"
        compiled = query_spec.sql_projection_compiler(["track"]).as_string()
        placeholders = set(re.findall(r"%\(([^)]+)\)s", compiled))
        assert all(placeholders <= values.keys() for values in params)
        executions.append(_Execution(query_spec, params, plan))
        return ((index, None) for index in reversed(range(len(params))))

    monkeypatch.setattr(query, "execute_batch_read_query_stream", fake_execute)
    return query, executions


def _nearest_results(
    query: AlongTrack,
    surface: Surface,
    radius_options: _RadiusOptions,
    missions: list[Mission] | None = None,
) -> list[Dataset[along_track_fields] | None]:
    # Keep every pre-existing positional argument in use.
    if surface == "scalar":
        return [
            query.geographic_nearest_neighbors(
                ["track"], 0.0, 3.0, DATE, WINDOW, missions, **radius_options
            )
        ]
    if surface == "batch":
        return list(
            query.geographic_nearest_neighbors_batch(
                ["track"],
                [0.0, 1.0, 2.0],
                [3.0, 4.0, 5.0],
                [DATE] * 3,
                WINDOW,
                missions,
                n_jobs=2,
                chunk_size=1,
                **radius_options,
            )
        )
    return [
        result
        for _, result in query.geographic_nearest_neighbors_stream(
            ["track"],
            [0.0, 1.0, 2.0],
            [3.0, 4.0, 5.0],
            [DATE] * 3,
            WINDOW,
            missions,
            n_jobs=2,
            chunk_size=1,
            **radius_options,
        )
    ]


@pytest.mark.parametrize(
    "method, coordinates, n_jobs",
    [
        (
            AlongTrack.geographic_nearest_neighbors,
            ("latitude", "longitude", "date"),
            None,
        ),
        (
            AlongTrack.geographic_nearest_neighbors_batch,
            ("latitudes", "longitudes", "dates"),
            1,
        ),
        (
            AlongTrack.geographic_nearest_neighbors_stream,
            ("latitudes", "longitudes", "dates"),
            4,
        ),
    ],
)
def test_nearest_radius_is_optional_keyword_only(
    method: Callable[..., object],
    coordinates: tuple[str, str, str],
    n_jobs: int | None,
) -> None:
    parameters = signature(method).parameters
    positional_names = ["self", "fields", *coordinates, "time_window", "missions"]
    assert list(parameters)[: len(positional_names)] == positional_names
    assert all(
        parameters[name].kind == Parameter.POSITIONAL_OR_KEYWORD
        for name in positional_names
    )
    assert parameters["time_window"].default == timedelta(days=10)
    assert parameters["missions"].default is None
    assert parameters["max_radius"].kind == Parameter.KEYWORD_ONLY
    assert parameters["max_radius"].default is None
    if n_jobs is not None:
        assert parameters["n_jobs"].kind == Parameter.KEYWORD_ONLY
        assert parameters["n_jobs"].default == n_jobs
        assert parameters["chunk_size"].kind == Parameter.KEYWORD_ONLY
        assert parameters["chunk_size"].default == 16


@pytest.mark.parametrize("surface", ["scalar", "batch", "stream"])
@pytest.mark.parametrize("missions", [None, ["j2"]])
@pytest.mark.parametrize(
    "radius_options",
    [{}, {"max_radius": None}, {"max_radius": 0.0}, {"max_radius": 111_000.0}],
    ids=["default-unbounded", "explicit-unbounded", "zero", "bounded"],
)
def test_nearest_radius_reaches_execute_boundary(
    captured_query: tuple[AlongTrack, list[_Execution]],
    surface: Surface,
    missions: list[Mission] | None,
    radius_options: _RadiusOptions,
) -> None:
    query, executions = captured_query
    count = 1 if surface == "scalar" else 3
    assert _nearest_results(query, surface, radius_options, missions) == [None] * count
    assert len(executions) == 1
    execution = executions[0]
    assert execution.query_spec.mandatory_fields == ["distance"]
    assert len(execution.params) == count
    for index, params in enumerate(execution.params):
        assert params["max_radius"] == radius_options.get("max_radius")
        assert params["latitude"] == float(index)
        assert params["longitude"] == float(index + 3)
        assert params["central_date_time"] == DATE
        assert params["time_delta"] == WINDOW
        assert params["connected_basin_ids"] == [1, 2]
        if missions is None:
            assert "missions" not in params
        else:
            assert params["missions"] == missions
    assert len(execution.plan) == (1 if surface == "scalar" else 2)
    assert len(set(execution.plan.chunks.values())) == count


@pytest.mark.parametrize("surface", ["scalar", "batch", "stream"])
@pytest.mark.parametrize(
    "max_radius", [-1.0, float("nan"), float("inf"), -float("inf")]
)
def test_invalid_nearest_radius_fails_before_query_building(
    captured_query: tuple[AlongTrack, list[_Execution]],
    monkeypatch: pytest.MonkeyPatch,
    surface: Surface,
    max_radius: float,
) -> None:
    query, executions = captured_query

    def fail_build(**kwargs: object) -> NoReturn:
        pytest.fail("Invalid max_radius must be rejected before basin/database access")

    monkeypatch.setattr(query, "_build_basin_query_batch", fail_build)
    with pytest.raises(ValueError, match="max_radius must be finite and non-negative"):
        _nearest_results(query, surface, {"max_radius": max_radius})
    assert not executions
