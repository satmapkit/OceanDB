"""Helpers for distributing read-query indices across worker jobs."""

from OceanDB.data_access.base_query import QueryPlan


def plan_by_basin(basin_ids: list[int], *, n_jobs: int, chunk_size: int) -> QueryPlan:
    """
    Group query points by lookup basin, chunk them, and balance jobs by size.
    There will be at least as many chunks as queried basins.

    basin_ids here are the lookup basins for each point, not the connected
    basin set.
    """
    if n_jobs < 1:
        raise ValueError("n_jobs must be at least 1")
    if chunk_size < 1:
        raise ValueError("chunk_size must be at least 1")

    basin_indices: dict[int, list[int]] = {}
    for index, basin in enumerate(basin_ids):
        basin_indices.setdefault(basin, []).append(index)

    # Keep chunks basin-homogeneous, splitting large basins as needed.
    chunk_indices: dict[int, list[int]] = {}
    for indices in basin_indices.values():
        for start in range(0, len(indices), chunk_size):
            chunk = len(chunk_indices)
            chunk_indices[chunk] = indices[start : start + chunk_size]

    chunks = {
        index: chunk for chunk, indices in chunk_indices.items() for index in indices
    }

    # Place larger chunks first on the currently least-loaded job.
    jobs: dict[int, int] = {}
    job_sizes = [0] * n_jobs
    for chunk, indices in sorted(
        chunk_indices.items(), key=lambda item: (-len(item[1]), item[0])
    ):
        job = min(
            range(n_jobs), key=lambda candidate: (job_sizes[candidate], candidate)
        )
        jobs[chunk] = job
        job_sizes[job] += len(indices)

    return QueryPlan(chunks, jobs)
