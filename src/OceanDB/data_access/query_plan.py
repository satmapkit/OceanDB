"""Helpers for grouping read-query indices into worker chunks."""

from abc import ABC, abstractmethod
from typing import Any

from OceanDB.data_access.base_query import QueryPlan


class QueryPlanner(ABC):
    @abstractmethod
    def plan(
        self, *, params_batch: list[dict[str, Any]], basin_ids: list[int]
    ) -> QueryPlan: ...


class SingleThreadPlanner(QueryPlanner):
    def plan(self, *, params_batch, basin_ids):
        return QueryPlan({i: 0 for i in range(len(params_batch))}, 1)


class BasinPlanner(QueryPlanner):
    n_jobs: int

    def __init__(self, n_jobs: int):
        if n_jobs < 1:
            raise ValueError("n_jobs must be at least 1")
        self.n_jobs = n_jobs

    def plan(self, *, params_batch, basin_ids):
        return self.plan_by_basin(basin_ids)

    def plan_by_basin(self, basin_ids: list[int]) -> QueryPlan:
        """
        Chunk query points by the basin each query point is in.
        There will be as many chunks as queried basins.
        """

        chunks = {i: basin_id for i, basin_id in enumerate(basin_ids)}
        return QueryPlan(chunks, self.n_jobs)


class LatitudePlanner(QueryPlanner):
    n_chunks: int
    n_jobs: int

    def __init__(self, n_jobs: int, n_chunks: int):
        if n_jobs < 1:
            raise ValueError("n_jobs must be at least 1")
        if n_chunks < 1:
            raise ValueError("n_chunks must be at least 1")

        self.n_jobs = n_jobs
        self.n_chunks = n_chunks

    def plan(self, *, params_batch, basin_ids):
        return self.plan_by_latitude([params["latitude"] for params in params_batch])

    def plan_by_latitude(self, latitudes: list[float]) -> QueryPlan:
        """
        Chunk query points by latitude
        """

        min_lat = min(latitudes)
        max_lat = max(latitudes)
        chunk_size = (max_lat - min_lat) / self.n_chunks + 1

        chunks = {
            i: int((latitude - min_lat) / chunk_size)
            for i, latitude in enumerate(latitudes)
        }

        return QueryPlan(chunks, self.n_jobs)
