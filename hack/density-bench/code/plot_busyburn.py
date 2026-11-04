import json, glob, os, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

R = os.path.expanduser("~/bench-runs/results")
OUT = os.path.dirname(os.path.abspath(__file__)) + "/../plots"
os.makedirs(OUT, exist_ok=True)

RUNS = {
    "gv": ("us_busyburn-gv_busyburn-20261028T003158Z/bm_gvisor", "A_gv_busyburn", "gVisor busy-burn (baseline)"),
    "gvp": ("us_busyburn_protect-gv_busyburn_protect-20261028T024306Z/bm_gvisor", "A_gv_busyburn_protect", "gVisor busy-burn + node-agent CPU protection"),
    "mv": ("z3_busyburn-mv_busyburn-20261027T233558Z/bm_microvm_z3", "A_mv_busyburn", "microVM busy-burn (baseline)"),
}


def load(key):
    d, pfx, title = RUNS[key]
    base = f"{R}/{d}/{pfx}"
    prom = json.load(open(base + "_prom_raw.json"))
    db = json.load(open(base + "_db_samples.json"))
    probes = []
    for f in sorted(glob.glob(f"{R}/{d}/probe_{pfx}_*u.txt")):
        for l in open(f):
            if l.startswith('{"type":"sample"'):
                probes.append(json.loads(l))
    return prom, db, probes, title


# Load-generator series (users, op rates) are reported twice in prom (2x the
# DB count / the 1-step-per-11s arithmetic), so they are halved.
DOUBLED = {"users", "ok_rate_by_op", "fail_rate_by_op"}


def series(prom, k, name=None):
    f = 0.5 if k in DOUBLED else 1.0
    for s in prom.get(k, []):
        if name is None or s["metric"].get("name") == name:
            return [v[0] for v in s["values"]], [float(v[1]) * f for v in s["values"]]
    return [], []


def gaps(ts, vs, maxgap=90):
    """Insert None where samples are missing so lines don't bridge gaps."""
    T, V = [], []
    for i, (t, v) in enumerate(zip(ts, vs)):
        if i and t - ts[i - 1] > maxgap:
            T.append(t - 1); V.append(None)
        T.append(t); V.append(v)
    return T, V


def probe_cpu(probes):
    out = []
    for a, b in zip(probes, probes[1:]):
        dt = b["t"] - a["t"]
        if 0 < dt < 60 and "root_cpu_usage_usec" in a:
            out.append((b["t"], (b["root_cpu_usage_usec"] - a["root_cpu_usage_usec"]) / 1e6 / dt))
    return out


def interp(tq, ts, vs):
    out = []
    j = 0
    for t in tq:
        while j + 1 < len(ts) and ts[j + 1] <= t:
            j += 1
        out.append(vs[j] if ts and ts[0] <= t else None)
    return out


def fig_run(key):
    prom, db, probes, title = load(key)
    t0 = db[0]["t"]
    m = lambda ts: [(t - t0) / 60 for t in ts]
    fig, ax = plt.subplots(5, 1, figsize=(11, 16), sharex=True)
    fig.suptitle(title + " - 192 vCPU node", fontsize=14)

    # A. counts
    a = ax[0]
    ut, uv = series(prom, "users")
    a.plot(m(ut), uv, "k--", label="actors requested (load users)")
    a.plot(m([x["t"] for x in db]), [x["running_assigned"] for x in db], label='control plane says "Running"')
    a.plot(m([x["t"] for x in db]), [x["registered"] for x in db], alpha=.6, label="registered in DB")
    lt, lv = series(prom, "running_atelet")
    gt, gv = gaps(lt, lv)
    a.plot(m(gt), gv, lw=2.5, label="actually live sandboxes (atelet)")
    a.set_ylabel("actors")
    a.set_title("A. How many actors exist")
    a.legend(loc="upper left", fontsize=8)

    # B. CPU
    b = ax[1]
    ct, cv = series(prom, "node_cpu")
    gt, gv = gaps(ct, cv)
    b.plot(m(gt), gv, label="node CPU used (prometheus, cgroup)")
    pc = probe_cpu(probes)
    b.scatter(m([x[0] for x in pc]), [x[1] for x in pc], s=6, color="purple", label="node CPU (node probe, root cgroup)")
    b.axhline(192, color="r", ls=":", label="192 vCPU")
    b.set_ylabel("cores")
    b.set_ylim(0, 210)
    b2 = b.twinx()
    pt, pv = series(prom, "psi_cpu")
    pt, pv = gaps(pt, pv)
    b2.plot(m(pt), pv, color="orange", alpha=.7, label="CPU pressure (PSI some, %)")
    b2.set_ylabel("PSI %")
    b.set_title("B. Node CPU (gaps = metrics scrape failed; node agents starved)")
    h1, l1 = b.get_legend_handles_labels(); h2, l2 = b2.get_legend_handles_labels()
    b.legend(h1 + h2, l1 + l2, loc="lower right", fontsize=8)

    # C. CPU per live actor
    c = ax[2]
    live = interp(ct, lt, lv)
    pts = [(t, cpu / n) for t, cpu, n in zip(ct, cv, live) if n and n > 20]
    if pts:
        gt, gv = gaps([p[0] for p in pts], [p[1] for p in pts])
        c.plot(m(gt), gv, "g.-")
    c.axhline(1.0, color="gray", ls=":", label="1 core = what each actor asks for")
    c.set_yscale("log")
    c.set_ylabel("cores per live actor")
    c.set_title("C. CPU each live actor actually gets (node CPU / live sandboxes)")
    c.legend(fontsize=8)

    # D. op rates
    d = ax[3]
    for k, nm, lab, st in [("ok_rate_by_op", "Step_burn", "burn steps done /s (each = 10 s wall-clock burn)", "-"),
                           ("ok_rate_by_op", "CreateActor", "CreateActor ok /s", "-"),
                           ("ok_rate_by_op", "DeleteActor", "DeleteActor ok /s", "-"),
                           ("fail_rate_by_op", "DeleteActor", "DeleteActor FAILED /s", "--"),
                           ("fail_rate_by_op", "Step_burn", "burn step FAILED /s", "--")]:
        t, v = series(prom, k, nm)
        if t:
            t, v = gaps(t, v)
            d.plot(m(t), v, st, label=lab)
    d.set_ylabel("ops / s")
    d.set_title("D. What actors are doing (load-generator op rates)")
    d.legend(loc="upper left", fontsize=8)

    # E. runnable tasks
    e = ax[4]
    if probes:
        e.scatter(m([p["t"] for p in probes]), [p.get("runnable_tasks", 0) for p in probes], s=10, label="runnable tasks (waiting for a CPU)")
        e.scatter(m([p["t"] for p in probes]), [p.get("loadavg1", 0) for p in probes], s=10, label="load average (1 min)")
    e.axhline(192, color="r", ls=":", label="192 vCPU")
    e.set_yscale("log")
    e.set_ylabel("tasks")
    e.set_title("E. CPU run queue (node probe; missing where the probe pod could not start)")
    e.legend(fontsize=8)
    e.set_xlabel("minutes since run start")
    fig.tight_layout()
    p = f"{OUT}/busyburn_{key}.png"
    fig.savefig(p, dpi=90)
    print(p)


def fig_steps():
    # per-step summary from driver live lines: total burn steps per step window, live
    import re
    fig, ax = plt.subplots(1, 2, figsize=(12, 4.5))
    for key, col in [("gv", "C0"), ("mv", "C1"), ("gvp", "C2")]:
        d, pfx, title = RUNS[key]
        prom, db, probes, _ = load(key)
        lt, lv = series(prom, "running_atelet")
        ct, cv = series(prom, "node_cpu")
        st, sv = series(prom, "ok_rate_by_op", "Step_burn")
        # bucket by requested users level
        ut, uv = series(prom, "users")
        lev = {}
        for t, u in zip(ut, uv):
            lev.setdefault(int(round(u / 100) * 100), []).append(t)
        xs, live, cpa = [], [], []
        for u in sorted(lev):
            ts = lev[u]
            if len(ts) < 6 or u < 100:
                continue
            L = [v for t, v in zip(lt, lv) if ts[0] <= t <= ts[-1]]
            C = [v for t, v in zip(ct, cv) if ts[0] <= t <= ts[-1]]
            if not L:
                continue
            xs.append(u); live.append(max(L))
            cpa.append((sum(C) / len(C)) / (sum(L) / len(L)) if C else None)
        ax[0].plot(xs, live, "o-", color=col, label=title)
        ax[1].plot([x for x, c in zip(xs, cpa) if c], [c for c in cpa if c], "o-", color=col, label=title)
    ax[0].plot([0, 4000], [0, 4000], "k:", label="live = requested")
    ax[0].set_xlabel("actors requested"); ax[0].set_ylabel("peak live sandboxes"); ax[0].set_title("Live vs requested")
    ax[1].axhline(1, color="gray", ls=":")
    ax[1].set_xlabel("actors requested"); ax[1].set_ylabel("cores per live actor"); ax[1].set_yscale("log"); ax[1].set_title("CPU each live actor gets")
    for a in ax:
        a.legend(fontsize=7)
    fig.tight_layout()
    p = f"{OUT}/busyburn_steps.png"
    fig.savefig(p, dpi=90)
    print(p)


for k in sys.argv[1:] or ["gv", "gvp", "mv"]:
    fig_run(k)
fig_steps()
