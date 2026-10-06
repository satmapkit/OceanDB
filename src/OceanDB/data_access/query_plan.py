"""Helpers for distributing read-query indices across worker jobs."""

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
        Group query points by lookup basin, chunk them, and balance jobs by size.
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

        # Place larger chunks first on the currently least-loaded job.
        jobs: dict[int, int] = {}
        job_sizes = [0] * self.n_jobs
        for chunk, indices in sorted(
            chunk_indices.items(), key=lambda item: (-len(item[1]), item[0])
        ):
            job = min(
                range(self.n_jobs),
                key=lambda candidate: (job_sizes[candidate], candidate),
            )
            jobs[chunk] = job
            job_sizes[job] += len(indices)

        return QueryPlan(chunks, jobs)


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

        jobs: dict[int, int] = {}
        job_sizes = [0] * self.n_jobs
        for start in range(0, len(sorted_indices), self.chunk_size):
            chunk = start // self.chunk_size
            job = min(
                range(self.n_jobs),
                key=lambda candidate: (job_sizes[candidate], candidate),
            )
            jobs[chunk] = job
            job_sizes[job] += min(self.chunk_size, len(sorted_indices) - start)

        return QueryPlan(chunks, jobs)
