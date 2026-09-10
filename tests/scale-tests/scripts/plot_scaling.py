"""Render the A1 scaling charts as PNGs.

Mermaid's xychart-beta cannot label series on the renderer this repo is read
with: `line "name" [...]` parses far enough to draw axes and then drops the data,
so a multi-series chart comes out empty. Unnamed series render but the legend
reads "Line 1..4", which is useless for comparing components. matplotlib gives a
real legend and lets each component keep its own marker, so the charts are
generated here and committed as images.

All values are container working set, measured after restarting each component so
it synced against the fleet it is being measured on. Points marked restart-
controlled in the report are the ones plotted; readings taken on pods that had
run at a different fleet size are excluded, because they carry the older fleet's
footprint.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

OUT = "/home/ajmishra/stuffs/NVSentinel/tests/scale-tests/results"

# nodes -> settled working set in GB, on an idle fleet of 731 pods, each
# component restarted at that fleet size so it is not carrying heap from a
# larger one. None where the point was not measured (janitor's cache only exists
# once a TerminateNode has been seen, so it has no meaningful reading below 25k).
#
# Settled, not peak. labeler previously carried 6.78 and 12.01 here, which were
# its sync peaks: it spikes about 3x while listing the fleet and then falls back.
# At 100,005 nodes that is 12.9 GB during the list against 4.1 GB settled. Mixing
# a peak into a series of settled values made it look like the largest component
# after kubernetes-object-monitor, which it is not. The peak still governs the
# memory limit and is recorded per component in A1.
NODES = [4933, 9990, 25005, 50005, 75005, 100005]
SERIES = {
    "kubernetes-object-monitor": [1.13, 1.84, 4.58, 11.76, 12.60, 21.24],
    "fault-quarantine":          [0.87, 1.66, 4.11, 8.21, 11.67, 16.17],
    "labeler":                   [0.28, 0.57, 1.80, 2.08, 3.10, 4.10],
    "janitor":                   [None, None, 0.50, 2.41, 2.74, 4.17],
    "node-drainer":              [0.04, 0.06, 0.09, 0.14, 0.34, 0.36],
}

# MongoDB resident memory, summed across all three replica-set members. One
# measured point: 30.6 + 25.3 + 29.1 = 85.0 GB at 50,021 nodes, against 350,174
# connections -- 237 KB per connection and 1.70 MB per node. Every other scale
# point is that figure scaled linearly, which is what the connection model
# implies and all a single measured point supports.
MONGO_MB_PER_NODE = 1.70
MONGO_MEASURED = (50021, 85.0)

# ADR-052 (docs/designs/052-deployment-platform-connector.md) replaces the
# per-node platform-connector DaemonSet with a ~3-replica Deployment that
# monitors reach over gRPC. The 7 connections a node opens across the replica
# set collapse to a fixed pool: ~16 per replica plus the ~20 the six central
# services already hold, about 70 in total at any fleet size. MongoDB's resident
# memory then stops tracking the fleet and falls back to its floor, measured
# directly by scaling connector-pool-mongo to zero: 1,401 + 461 + 470 = 2,332 MB
# across the three members at 180 connections.
MONGO_FLOOR_AFTER_ADR052 = 2.33

# The cost does not vanish, it moves. Every monitor pod holds one HTTP/2
# connection to a replica, ~3 per node, at "tens of kilobytes" of server memory
# each for the read and write buffers (ADR-052, "Connections"). At 32 KB that is
# ~0.1 MB/node, which is where the ADR's "several GiB per replica" at 100,000
# nodes comes from. Tunable via the buffer sizes; the ADR defers the real figure
# to a load test, so this band is a projection, not a measurement.
GRPC_CONNS_PER_NODE = 3
GRPC_BYTES_PER_CONN = 32 * 1024

CP_NODES = [10000, 25000, 50000, 100000]

# Whole-system component totals on the same idle fleet as SERIES above, so the
# stacked bands and the total line share one basis. Mixing bases here produced a
# 16 GB "other components" wedge that was really the gap between idle bands and a
# loaded total.
#
# This chart is therefore the idle-fleet picture. A loaded cluster is materially
# larger -- at 100,005 nodes with a pod on every node the components sum to
# 62.6 GB rather than 54.0, giving a control plane of about 233 GB. That figure
# is in the summary; it is not plotted because the smaller scale points were
# never measured with a realistic pod population.
CP_TOTAL_COMPONENTS = {10000: 4.6, 25000: 11.1, 50000: 24.7, 100000: 54.0}

# janitor below 25k: its Node cache is created lazily on the first TerminateNode
# reconcile, so an unseeded pod reads ~0.055 GB at any fleet size.
JANITOR_UNSEEDED = 0.055


def plot_components():
    fig, ax = plt.subplots(figsize=(9, 5.5))
    markers = ["o", "s", "^", "D", "v"]
    for (name, vals), m in zip(SERIES.items(), markers):
        xs = [n for n, v in zip(NODES, vals) if v is not None]
        ys = [v for v in vals if v is not None]
        ax.plot(xs, ys, marker=m, linewidth=2, markersize=6, label=name)
    ax.set_xlabel("Nodes")
    ax.set_ylabel("Working set (GB)")
    ax.set_title("NVSentinel component memory vs fleet size\n"
                 "settled container working set on an idle fleet (731 pods), restarted at each point",
                 fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.legend(frameon=False)
    ax.set_xlim(0, 105000)
    ax.set_ylim(0, 23)
    fig.tight_layout()
    fig.savefig(f"{OUT}/component-memory-vs-nodes.png", dpi=140)
    print("wrote component-memory-vs-nodes.png")


def plot_control_plane():
    """Stacked areas per component, with MongoDB as the bottom band.

    A stack rather than side-by-side bars because the question the chart answers
    is what the whole control plane costs at a given fleet size and which part of
    it dominates; the top edge of the stack is that total, read directly.
    """
    fig, ax = plt.subplots(figsize=(9.5, 5.5))

    xs = NODES
    mongo = [MONGO_MB_PER_NODE * n / 1000 for n in xs]

    bands = [("MongoDB (3 members combined, 1.70 MB/node)", mongo)]
    for name, vals in SERIES.items():
        bands.append((name, [JANITOR_UNSEEDED if v is None else v for v in vals]))

    # everything else, as the residual against the measured whole-system totals
    total_fit = np.polyfit(list(CP_TOTAL_COMPONENTS), list(CP_TOTAL_COMPONENTS.values()), 1)
    named = [sum(b[1][i] for b in bands[1:]) for i in range(len(xs))]
    other = [max(0.0, t - n) for t, n in zip(np.polyval(total_fit, xs), named)]
    bands.append(("other components", other))

    ax.stackplot(xs, [b[1] for b in bands], labels=[b[0] for b in bands], alpha=0.85)

    totals = [sum(b[1][i] for b in bands) for i in range(len(xs))]
    ax.plot(xs, totals, color="black", linewidth=1.6, marker="o", markersize=4,
            label="control plane total")
    for x, t in zip(xs, totals):
        ax.annotate(f"{t:.0f} GB", (x, t), textcoords="offset points",
                    xytext=(0, 8), ha="center", fontsize=9)

    ax.set_xlabel("Nodes")
    ax.set_ylabel("Memory (GB)")
    ax.set_title("Control-plane memory by component, idle fleet (731 pods)\n"
                 "MongoDB is combined across the replica set and dominates at every scale point",
                 fontsize=11)
    ax.set_xlim(0, 108000)
    ax.set_ylim(0, max(totals) * 1.12)
    ticks = sorted(CP_NODES + [5000, 75000])
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{n//1000}k" for n in ticks])
    ax.grid(True, axis="y", alpha=0.3)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], frameon=False, loc="upper left", fontsize=9)
    fig.tight_layout()
    fig.savefig(f"{OUT}/control-plane-memory.png", dpi=140)
    print("wrote control-plane-memory.png")


def plot_adr052():
    """Projected control-plane memory once ADR-052 lands, against today's.

    Nothing here is measured. The component bands are the measured ones, which
    ADR-052 does not touch; what changes is that MongoDB stops scaling with the
    fleet and a new gRPC-connection band appears in its place.
    """
    fig, ax = plt.subplots(figsize=(9.5, 5.5))
    xs = NODES

    bands = [
        ("MongoDB (fixed pool, ~70 connections)", [MONGO_FLOOR_AFTER_ADR052] * len(xs)),
        ("platform-connector-deployment (gRPC buffers)",
         [GRPC_CONNS_PER_NODE * n * GRPC_BYTES_PER_CONN / 1e9 for n in xs]),
    ]
    for name, vals in SERIES.items():
        bands.append((name, [JANITOR_UNSEEDED if v is None else v for v in vals]))
    total_fit = np.polyfit(list(CP_TOTAL_COMPONENTS), list(CP_TOTAL_COMPONENTS.values()), 1)
    named = [sum(b[1][i] for b in bands[2:]) for i in range(len(xs))]
    bands.append(("other components",
                  [max(0.0, t - n) for t, n in zip(np.polyval(total_fit, xs), named)]))

    ax.stackplot(xs, [b[1] for b in bands], labels=[b[0] for b in bands], alpha=0.85)

    after = [sum(b[1][i] for b in bands) for i in range(len(xs))]
    before = [MONGO_MB_PER_NODE * n / 1000 + np.polyval(total_fit, n) for n in xs]
    ax.plot(xs, before, color="black", linestyle="--", linewidth=1.6,
            label="today (DaemonSet platform connector)")
    ax.plot(xs, after, color="black", linewidth=1.8, marker="o", markersize=4,
            label="projected total")
    for x, a, b in zip(xs, after, before):
        ax.annotate(f"{a:.0f} GB", (x, a), textcoords="offset points",
                    xytext=(0, 8), ha="center", fontsize=9)
    ax.annotate(f"{before[-1]:.0f} GB", (xs[-1], before[-1]), textcoords="offset points",
                xytext=(-6, 6), ha="right", fontsize=9)

    ax.set_xlabel("Nodes")
    ax.set_ylabel("Memory (GB)")
    ax.set_title("Control-plane memory after ADR-052, projected\n"
                 "MongoDB stops scaling with the fleet; the connection cost moves into the gRPC server",
                 fontsize=11)
    ax.set_xlim(0, 105000)
    ax.set_ylim(0, max(max(before), max(after)) * 1.12)
    ticks = sorted(CP_NODES + [5000, 75000])
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{n//1000}k" for n in ticks])
    ax.grid(True, axis="y", alpha=0.3)
    h, l = ax.get_legend_handles_labels()
    ax.legend(h[::-1], l[::-1], frameon=False, loc="upper left", fontsize=9)
    fig.tight_layout()
    fig.savefig(f"{OUT}/control-plane-memory-adr052.png", dpi=140)
    print("wrote control-plane-memory-adr052.png")


if __name__ == "__main__":
    plot_components()
    plot_control_plane()
    plot_adr052()
