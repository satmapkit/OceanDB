import random

import pytest

from OceanDB.data_access.base_query import BaseReadQuery, QueryPlan
from OceanDB.data_access.query_plan import BasinPlanner, LatitudePlanner
from OceanDB.query_spec import QuerySpec


def test_plan_good():
    for _ in range(100):
        n_indices = random.randint(100, 200)
        n_chunks = random.randint(1, 50)
        n_jobs = random.randint(1, 20)
        plan = QueryPlan(
            {i: i % n_chunks for i in range(n_indices)},
            {c: c % n_jobs for c in range(n_chunks)},
        )

        plan.validate(n_indices)
        jobs = plan.job_chunks()
        for i, job in enumerate(jobs):
            chunk_0 = job[0]
            assert chunk_0 == [x for x in range(n_indices) if x % n_chunks == i]

        with pytest.raises(ValueError):
            plan.validate(n_indices - 1)
        with pytest.raises(ValueError):
            plan.validate(n_indices + 1)
        with pytest.raises(ValueError):
            plan.validate(0)


def test_plan_bad_missing_index():
    n_indices = 5
    for missing_index in range(n_indices):
        chunks = {i: 0 for i in range(n_indices) if i != missing_index}
        jobs = {0: 0}
        plan = QueryPlan(chunks, jobs)
        with pytest.raises(ValueError):
            plan.validate(n_indices)


def test_plan_bad_missing_chunk():
    n_chunks = 5
    for missing_chunk in range(n_chunks):
        chunks = {i: i for i in range(n_chunks)}  # 1 index per chunk
        jobs = {
            i: 0 for i in range(n_chunks) if i != missing_chunk
        }  # 1 job for all chunks
        plan = QueryPlan(chunks, jobs)
        with pytest.raises(ValueError):
            plan.validate(n_chunks)


def test_plan_bad_extra_chunk():
    n_indices = 2
    chunks = {i: i for i in range(n_indices)}  # 1 index per chunk

    jobs = {i: 0 for i in range(n_indices)}  # 1 job for all chunks
    jobs[n_indices + 1] = 0  # and an extra chunk

    plan = QueryPlan(chunks, jobs)
    with pytest.raises(ValueError):
        plan.validate(n_indices)


def test_execute_batch_read_query_stream_emits_indexed_results(monkeypatch):
    query = BaseReadQuery.__new__(BaseReadQuery)
    calls = []

    def fake_execute_batch_read_query(
        *, query_spec, fields, params_batch, dataset_name
    ):
        print("in fake_execute_batch_query()")
        params = list(params_batch)
        calls.append(params)
        return (param["value"] * 2 for param in params)

    monkeypatch.setattr(
        query, "execute_batch_read_query", fake_execute_batch_read_query
    )

    plan = QueryPlan(
        {
            0: 0,
            2: 0,
            1: 1,
            3: 1,
            4: 2,
        },
        {
            0: 0,
            2: 0,
            1: 1,
        },
    )

    print("about to read stream")
    streamed = list(
        query.execute_batch_read_query_stream(
            query_spec=QuerySpec("", {}),
            fields=[],
            params_batch=[{"value": index} for index in range(5)],
            plan=plan,
        )
    )

    assert sorted(streamed) == [(index, index * 2) for index in range(5)]
    assert sorted(tuple(param["value"] for param in call) for call in calls) == [
        (0, 2),
        (1, 3),
        (4,),
    ]


def test_plan_by_basin_groups_by_lookup_basin_and_assigns_every_index():
    basin_ids = [1, 2, 1, 1, 3, 2, 1]

    plan = BasinPlanner(2, 2).plan(
        params_batch=[{} for _ in basin_ids], basin_ids=basin_ids
    )
    plan.validate(len(basin_ids))

    chunk_indices = {}
    for index, chunk in plan.chunks.items():
        chunk_indices.setdefault(chunk, []).append(index)
    assert sorted(
        index for indices in chunk_indices.values() for index in indices
    ) == list(range(len(basin_ids)))
    for indices in chunk_indices.values():
        assert len(indices) <= 2
        assert len({basin_ids[index] for index in indices}) == 1

    # The greedy assignment should distribute this workload across both jobs.
    assert set(plan.jobs.values()) == {0, 1}


def test_plan_by_basin_rejects_invalid_limits():
    for n_jobs, chunk_size in [(0, 1), (1, 0)]:
        with pytest.raises(ValueError):
            BasinPlanner(n_jobs, chunk_size).plan(params_batch=[{}], basin_ids=[1])


def test_plan_by_latitude_groups_sorted_points_and_balances_jobs():
    latitudes = [40.0, -20.0, 10.0, -20.0, 0.0, 30.0, 20.0]
    plan = LatitudePlanner(2, 2).plan(
        params_batch=[{"latitude": latitude} for latitude in latitudes], basin_ids=[]
    )
    plan.validate(len(latitudes))

    assert len(plan.jobs) == 4
    assert plan.job_chunks() == [[[1, 3], [6, 5]], [[4, 2], [0]]]
    assert set(plan.jobs.values()) == {0, 1}


def test_plan_by_latitude_empty_and_invalid_limits():
    plan = LatitudePlanner(2, 2).plan(params_batch=[], basin_ids=[])
    plan.validate(0)
    assert plan.chunks == plan.jobs == {}

    for n_jobs, chunk_size in [(0, 1), (1, 0)]:
        with pytest.raises(ValueError):
            LatitudePlanner(n_jobs, chunk_size).plan(
                params_batch=[{"latitude": 1.0}], basin_ids=[]
            )
