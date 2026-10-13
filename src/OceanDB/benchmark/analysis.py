"""Descriptive episode-level analysis; no ranking or tolerance gate."""

import statistics
from collections import Counter, defaultdict


def describe(xs):
    if not xs:
        return dict(n=0)
    mean = statistics.mean(xs)
    return dict(
        n=len(xs),
        median_s=statistics.median(xs),
        min_s=min(xs),
        max_s=max(xs),
        cv_pct=100 * statistics.stdev(xs) / mean if len(xs) > 1 and mean else None,
    )


def ratio(xs):
    return (
        dict(n=len(xs), median=statistics.median(xs), min=min(xs), max=max(xs))
        if xs
        else dict(n=0)
    )


def tables(results):
    lookup = {j["id"]: j for j in results["jobs"]}
    episodes = []
    pairs = defaultdict(dict)
    cases = []
    for e in results["episodes"]:
        js = [lookup[j["id"]] for j in e["jobs"]]
        complete = bool(js) and all(j["status"] == "completed" for j in js)
        r = {
            k: e.get(k)
            for k in (
                "id",
                "configuration",
                "workflow",
                "scenario",
                "dates",
                "sample",
                "trial",
                "stage",
                "pair_order",
            )
        }
        r.update(
            complete=complete,
            seconds=sum(j["seconds"] for j in js) if complete else None,
            statuses=dict(Counter(j["status"] for j in js)),
            segment=js[0].get("segment", "unknown"),
            batch_count=len(js),
        )
        episodes.append(r)
        if e["configuration"] in ("ad_a", "ad_b"):
            pairs[(e["group"], e["trial"])][e["configuration"]] = (r, js)
        if e["trial"] == 0:
            for j in js:
                cases.append(
                    dict(
                        j,
                        location=j["date"]
                        or f"Points {j['point_indices'][0]+1:03d}–{j['point_indices'][-1]+1:03d}",
                        expected_batches=len(js),
                    )
                )
    groups = defaultdict(list)
    detail = defaultdict(list)
    for pair in pairs.values():
        if set(pair) != {"ad_a", "ad_b"}:
            continue
        (a, ajs), (b, bjs) = [pair[k] for k in ("ad_a", "ad_b")]

        def record(x, y):
            return dict(
                trial=a["trial"],
                segment=a["segment"],
                order=a["pair_order"],
                a_seconds=x,
                b_seconds=y,
                ratio=x / y,
            )

        key = (a["workflow"], a["scenario"])
        if a["complete"] and b["complete"]:
            groups[key].append(record(a["seconds"], b["seconds"]))
        for x, y in zip(ajs, bjs, strict=True):
            if x["status"] == y["status"] == "completed":
                loc = (
                    x["date"]
                    or f"Points {x['point_indices'][0]+1:03d}–{x['point_indices'][-1]+1:03d}"
                )
                detail[(*key, loc)].append(record(x["seconds"], y["seconds"]))

    def summary(key, rows):
        return dict(
            workflow=key[0],
            scenario=key[1],
            date=key[2] if len(key) > 2 else None,
            AD_A=describe([r["a_seconds"] for r in rows]),
            AD_B=describe([r["b_seconds"] for r in rows]),
            ratio=ratio([r["ratio"] for r in rows]),
            by_order={
                o: ratio([r["ratio"] for r in rows if r["order"] == o])
                for o in ("AB", "BA")
            },
            by_segment={
                s: ratio([r["ratio"] for r in rows if r["segment"] == s])
                for s in sorted({r["segment"] for r in rows})
            },
            trials=rows,
        )

    return dict(
        episodes=episodes,
        exploratory_cases=cases,
        aa_episodes=[summary(k, v) for k, v in groups.items()],
        aa_dates=[
            summary(k, v) for k, v in detail.items() if k[0] != "all_record_local_grid"
        ],
        aa_chunks=[
            summary(k, v) for k, v in detail.items() if k[0] == "all_record_local_grid"
        ],
    )
