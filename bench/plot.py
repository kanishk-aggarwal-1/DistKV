"""Charts for bench/summarize.py (static PNGs for the README).

Palette: categorical slots 1-2 of the reference data-viz palette, validated
(CVD ΔE 24.7, normal-vision ΔE 33.6, both ≥ 3:1 on the surface). Colour
follows the system, never the rank: DistKV is always blue, Redis always
orange. One y-axis per panel; small multiples where scales differ.
"""

import csv

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402

SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
BLUE = "#2a78d6"    # DistKV
ORANGE = "#eb6834"  # Redis
NEUTRAL = "#8a8983"

SYSTEM_COLOR = {"distkv": BLUE, "redis": ORANGE}
SYSTEM_LABEL = {"distkv": "DistKV", "redis": "Redis"}
TOPOLOGY_LABEL = {"single": "single node", "cluster": "cluster\n3 shards + replicas"}


def _style(ax):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=9, length=0)
    ax.yaxis.grid(True, color=GRID, linewidth=1)
    ax.set_axisbelow(True)


def _ops_tick(value, _pos):
    if value >= 1e6:
        return f"{value / 1e6:.1f}M"
    return f"{value / 1e3:.0f}k" if value else "0"


def _fmt(value, metric):
    if metric == "ops":
        return f"{value / 1000:,.0f}k" if value >= 10000 else f"{value:,.0f}"
    return f"{value:.2f}"


def benchmark_chart(rows, path):
    """Rows × pipelines grid: throughput, p50, p99. Bars = median, whiskers = min–max."""
    metrics = [("ops", "Throughput (ops/sec)"), ("p50", "p50 latency (ms)"), ("p99", "p99 latency (ms)")]
    pipelines = sorted({r["pipeline"] for r in rows})
    by_key = {(r["target"], r["pipeline"]): r for r in rows}

    fig, axes = plt.subplots(len(metrics), len(pipelines), figsize=(4.6 * len(pipelines), 3.2 * len(metrics)),
                             squeeze=False)
    fig.patch.set_facecolor(SURFACE)
    width, gap = 0.34, 0.02
    for row_i, (metric, title) in enumerate(metrics):
        for col_i, pipeline in enumerate(pipelines):
            ax = axes[row_i][col_i]
            _style(ax)
            for x, topology in enumerate(("single", "cluster")):
                for offset, system in ((-(width + gap) / 2, "distkv"), ((width + gap) / 2, "redis")):
                    r = by_key.get((f"{system}-{topology}", pipeline))
                    if r is None:
                        continue
                    med = r[f"{metric}_median"]
                    lo, hi = r[f"{metric}_min"], r[f"{metric}_max"]
                    ax.bar(x + offset, med, width, color=SYSTEM_COLOR[system],
                           label=SYSTEM_LABEL[system] if x == 0 else None)
                    ax.errorbar(x + offset, med, yerr=[[med - lo], [hi - med]], fmt="none",
                                ecolor=TEXT_SECONDARY, elinewidth=1, capsize=3)
                    ax.annotate(_fmt(med, metric), (x + offset, hi), textcoords="offset points",
                                xytext=(0, 4), ha="center", fontsize=8, color=TEXT)
            ax.set_xticks([0, 1], [TOPOLOGY_LABEL["single"], TOPOLOGY_LABEL["cluster"]])
            if metric == "ops":
                ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(_ops_tick))
            ax.set_title(f"{title} · pipeline {pipeline}", fontsize=10, color=TEXT, loc="left")
            ax.margins(y=0.15)
            ax.set_ylim(bottom=0)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", frameon=False, ncol=2, fontsize=9, labelcolor=TEXT)
    fig.text(0.01, 0.005, "Bars: median of runs; whiskers: min–max across runs.", fontsize=8, color=TEXT_SECONDARY)
    fig.tight_layout(rect=(0, 0.02, 1, 0.97))
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)


def failover_chart(summary, timeline_path, path):
    """Acknowledged probe writes per second around the kill: the probe writing
    to the failed group's slots vs the probe writing to the other groups'
    slots. The two probes write at the same pace, so the lines compare
    directly (same scale, one axis)."""
    t, victim, bystander = [], [], []
    with open(timeline_path) as f:
        for row in csv.DictReader(f):
            t.append(int(row["t_ms"]) / 1000)
            victim.append(int(row["victim_probe_acked"]) * 10)  # per 100 ms -> per second
            bystander.append(int(row["bystander_acked"]) * 10)

    fig, ax = plt.subplots(figsize=(8, 3.4))
    fig.patch.set_facecolor(SURFACE)
    _style(ax)
    ax.plot(t, bystander, color=ORANGE, linewidth=2, label="other groups' slots")
    ax.plot(t, victim, color=BLUE, linewidth=2, label="failed group's slots")
    kill = summary["kill_issued_ms"] / 1000
    ax.axvline(kill, color=NEUTRAL, linewidth=1)
    ax.annotate("primary killed", (kill, 1), xycoords=("data", "axes fraction"), xytext=(4, -12),
                textcoords="offset points", fontsize=8, color=TEXT_SECONDARY)
    if summary["failover_ms"] is not None:
        back = kill + summary["failover_ms"] / 1000
        ax.annotate(f"writes back after {summary['failover_ms']} ms", (back, 0.9), xycoords=("data", "axes fraction"),
                    xytext=(4, -12), textcoords="offset points", fontsize=8, color=TEXT)
    ax.set_xlabel("seconds since start", fontsize=9, color=TEXT_SECONDARY)
    ax.set_title(f"Probe writes acknowledged per second · round {summary['round']} · "
                 f"lost: {summary['lost']}", fontsize=10, color=TEXT, loc="left", pad=26)
    ax.set_ylim(bottom=0, top=max(max(victim, default=0), max(bystander, default=0), 1) * 1.35)
    # Legend above the plot area, where it cannot cover the lines.
    ax.legend(frameon=False, fontsize=9, labelcolor=TEXT, loc="lower left", bbox_to_anchor=(0, 1.02),
              ncol=2, borderaxespad=0)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)
