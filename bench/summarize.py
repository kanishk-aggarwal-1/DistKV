#!/usr/bin/env python3
"""Summarises a results directory and draws its charts.

    bench/summarize.py RESULTS_DIR

Reads only files in RESULTS_DIR:
  - memtier JSON files named TARGET-pPIPELINE-rRUN.json (run_benchmarks.sh)
  - failover-ROUND.json / .csv from distkv-verifier (run_failover_test.sh)
and writes summary.csv, summary.md and charts/*.png next to them. Every number
in the outputs comes from those files; nothing is typed in.
"""

import csv
import json
import re
import statistics
import sys
from pathlib import Path

BENCH_FILE = re.compile(r"^(?P<target>.+)-p(?P<pipeline>\d+)-r(?P<run>\d+)\.json$")
FAILOVER_FILE = re.compile(r"^failover-(?P<round>\d+)\.json$")


def load_benchmarks(results: Path):
    """{(target, pipeline): [ {ops, p50, p99, p999}, ... per run ]}"""
    runs = {}
    for path in sorted(results.glob("*.json")):
        m = BENCH_FILE.match(path.name)
        if not m:
            continue
        totals = json.loads(path.read_text())["ALL STATS"]["Totals"]
        pct = totals["Percentile Latencies"]
        runs.setdefault((m["target"], int(m["pipeline"])), []).append({
            "ops": totals["Ops/sec"],
            "p50": pct["p50.00"],
            "p99": pct["p99.00"],
            "p999": pct["p99.90"],
        })
    return runs


def spread(values):
    return statistics.median(values), min(values), max(values)


def summarize_benchmarks(results: Path, runs):
    rows = []
    for (target, pipeline), samples in sorted(runs.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        row = {"target": target, "pipeline": pipeline, "runs": len(samples)}
        for metric in ("ops", "p50", "p99", "p999"):
            med, lo, hi = spread([s[metric] for s in samples])
            row.update({f"{metric}_median": med, f"{metric}_min": lo, f"{metric}_max": hi})
        rows.append(row)

    with open(results / "summary.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    lines = ["| Target | Pipeline | Runs | Ops/sec (median [min–max]) | p50 ms | p99 ms | p99.9 ms |",
             "|---|---|---|---|---|---|---|"]
    for r in rows:
        lines.append(
            f"| {r['target']} | {r['pipeline']} | {r['runs']} "
            f"| {r['ops_median']:,.0f} [{r['ops_min']:,.0f}–{r['ops_max']:,.0f}] "
            f"| {r['p50_median']:.2f} [{r['p50_min']:.2f}–{r['p50_max']:.2f}] "
            f"| {r['p99_median']:.2f} [{r['p99_min']:.2f}–{r['p99_max']:.2f}] "
            f"| {r['p999_median']:.2f} [{r['p999_min']:.2f}–{r['p999_max']:.2f}] |")
    return rows, lines


def summarize_failover(results: Path):
    rounds = []
    for path in sorted(results.glob("failover-*.json"), key=lambda p: int(FAILOVER_FILE.match(p.name)["round"])):
        d = json.loads(path.read_text())
        d["round"] = int(FAILOVER_FILE.match(path.name)["round"])
        rounds.append(d)
    if not rounds:
        return rounds, []
    lines = ["| Round | Victim | Failover ms (kill → first ack) | Longest gap, victim slots ms "
             "| Longest gap, other groups ms | Write rate before kill /s | Acked | Refused "
             "| Ambiguous (present after) | **Lost** |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for d in rounds:
        # Acknowledged writes per second before the kill, from the timeline:
        # shows whether throughput holds up from round to round.
        rate = "—"
        timeline = results / f"failover-{d['round']}.csv"
        if timeline.exists() and d.get("kill_issued_ms"):
            with open(timeline) as f:
                before = [int(r["acked"]) for r in csv.DictReader(f) if int(r["t_ms"]) < d["kill_issued_ms"]]
            if before:
                rate = f"{sum(before) / (len(before) * 0.1):,.0f}"
        # The kill landed between issuing and the command returning: show both ends.
        if d["failover_ms"] is None:
            fo = "—"
        elif d.get("failover_from_return_ms") is not None:
            fo = f"{d['failover_from_return_ms']}–{d['failover_ms']}"
        else:
            fo = f"{d['failover_ms']}"
        lines.append(
            f"| {d['round']} | {d['victim']} | {fo} | {d['longest_victim_gap_ms']} "
            f"| {d.get('longest_bystander_gap_ms', '—')} | {rate} | {d['acked']:,} | {d['refused']:,} "
            f"| {d['ambiguous']} ({d['ambiguous_present']}) | **{d['lost']}** |")
    times = [d["failover_ms"] for d in rounds if d["failover_ms"] is not None]
    if times:
        med, lo, hi = spread(times)
        lines.append("")
        lines.append(f"Failover over {len(times)} round(s), measured from issuing the kill (upper bound): "
                     f"median {med:.0f} ms, min {lo} ms, max {hi} ms. "
                     f"Acknowledged writes lost across all rounds: **{sum(d['lost'] for d in rounds)}** "
                     f"of {sum(d['acked'] for d in rounds):,}.")
    return rounds, lines


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    results = Path(sys.argv[1])
    out = []
    runs = load_benchmarks(results)
    if runs:
        rows, lines = summarize_benchmarks(results, runs)
        out += ["## Benchmarks", "", *lines, ""]
    rounds, lines = summarize_failover(results)
    if rounds:
        out += ["## Failover", "", *lines, ""]
    if not out:
        sys.exit(f"no benchmark or failover files in {results}")
    env = results / "environment.txt"
    if env.exists():
        out = ["## Environment", "", "```", env.read_text().rstrip(), "```", ""] + out
    (results / "summary.md").write_text("\n".join(out) + "\n")
    print("\n".join(out))

    try:
        import plot
    except ImportError as e:  # matplotlib missing: summaries are still written
        print(f"(charts skipped: {e})")
        return
    charts = results / "charts"
    charts.mkdir(exist_ok=True)
    if runs:
        plot.benchmark_chart(rows, charts / "benchmark.png")
    for d in rounds:
        timeline = results / f"failover-{d['round']}.csv"
        if timeline.exists():
            plot.failover_chart(d, timeline, charts / f"failover-{d['round']}.png")
    print(f"charts in {charts}")


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    main()
