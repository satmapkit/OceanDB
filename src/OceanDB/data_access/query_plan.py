"""Helpers for grouping read-query indices into worker chunks."""

from abc import ABC, abstractmethod
from typing import Any

from OceanDB.data_access.base_query import QueryPlan


class QueryPlanner(ABC):
    n_jobs: int
    chunk_size: int

    def __init__(self, n_jobs: int, chunk_size: int):
        self.n_jobs = n_jobs
        self.chunk_size = chunk_size

    @abstractmethod
    def plan(
        self, *, params_batch: list[dict[str, Any]], basin_ids: list[int]
    ) -> QueryPlan: ...


class BasinPlanner(QueryPlanner):
    def plan(self, *, params_batch, basin_ids):
        return self.plan_by_basin(basin_ids)

    def plan_by_basin(self, basin_ids: list[int]) -> QueryPlan:
        """
        Group query points by lookup basin and chunk them.
        There will be at least as many chunks as queried basins.

        basin_ids here are the lookup basins for each point, not the connected
        basin set.
        """
        if self.n_jobs < 1:
            raise ValueError("n_jobs must be at least 1")
        if self.chunk_size < 1:
            raise ValueError("chunk_size must be at least 1")

        basin_indices: dict[int, list[int]] = {}
        for index, basin in enumerate(basin_ids):
            basin_indices.setdefault(basin, []).append(index)

        # Keep chunks basin-homogeneous, splitting large basins as needed.
        chunk_indices: dict[int, list[int]] = {}
        for indices in basin_indices.values():
            for start in range(0, len(indices), self.chunk_size):
                chunk = len(chunk_indices)
                chunk_indices[chunk] = indices[start : start + self.chunk_size]

        chunks = {
            index: chunk
            for chunk, indices in chunk_indices.items()
            for index in indices
        }

        return QueryPlan(chunks, self.n_jobs)


class LatitudePlanner(QueryPlanner):
    def plan(self, *, params_batch, basin_ids):
        return self.plan_by_latitude([params["latitude"] for params in params_batch])

    def plan_by_latitude(self, latitudes: list[float]) -> QueryPlan:
        """
        Group query points by latitude into
        ceil(# points / chunk_size) chunks.
        """
        if self.n_jobs < 1:
            raise ValueError("n_jobs must be at least 1")
        if self.chunk_size < 1:
            raise ValueError("chunk_size must be at least 1")

        sorted_indices = sorted(
            range(len(latitudes)), key=lambda index: latitudes[index]
        )
        chunks = {
            index: start // self.chunk_size
            for start in range(0, len(sorted_indices), self.chunk_size)
            for index in sorted_indices[start : start + self.chunk_size]
        }

        return QueryPlan(chunks, self.n_jobs)
