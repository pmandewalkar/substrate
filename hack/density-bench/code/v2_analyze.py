#!/usr/bin/env python3
"""Baseline v2 / A/B pass-fail analyzer.

usage: v2_analyze.py <glob of run dirs> [--out file.md]
Per step (from the run's summary windows):
  start_fail%  = start failures (runner log 'start failed' / 'on_start failed', by log timestamp) / max(actors requested, sessions ready)
  req_fail%    = summary fail_rate (request failures)
  live         = gVisor: probe procs_exe (sentry processes), median in window; else prom running_atelet avg
  db           = DB running_assigned median in window
  gap%         = (db - live) / db   (silent deaths / zombies)
  notready     = NodeNotReady events whose time falls in the window
  mem slope    = MemAvailable GiB/min over the last 10 min of the window (LPA only)
Clean = start_fail < 0.5% and req_fail < 1% and |gap| < 2% and notready == 0 and (not LPA or slope > -0.5 GiB/min).
"""
import glob, json, re, statistics, sys, datetime, os, io, subprocess

ARGS = sys.argv[1:]
OUT = None
if "--out" in ARGS:
    i = ARGS.index("--out"); OUT = ARGS[i + 1]; del ARGS[i:i + 2]

TS = re.compile(r'"time":"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)')
def iso(s):
    return datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone.utc).timestamp()

def runner_events(run):
    files = glob.glob(run + "/logs/runner_*.log") or glob.glob(run + "/*/*_runner_logs.txt")
    fails, ready = [], []
    for f in files:
        with open(f, errors="replace") as fh:
            for line in fh:
                if "start failed" in line:
                    m = TS.search(line)
                    if m: fails.append(iso(m.group(1)))
                elif "session is ready" in line or "Sweperf user session is ready" in line:
                    m = TS.search(line)
                    if m: ready.append(iso(m.group(1)))
    return sorted(set(fails)) if False else fails, ready

AGE = re.compile(r'^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$')
def node_flaps(run):
    """[(start, duration_s)] NotReady flaps of the metal node, paired with the next NodeReady."""
    f = run + "/logs/events.txt"
    if not os.path.exists(f):
        return []
    nr, rd = set(), set()
    snap = None
    for line in open(f, errors="replace"):
        if line.startswith("### "):
            try: snap = iso(line[4:23])
            except Exception: snap = None
            continue
        if not snap or "c3-metal" not in line:
            continue
        kind = "NodeNotReady" if "NodeNotReady" in line else ("NodeReady" if " NodeReady " in line else None)
        if not kind:
            continue
        parts = line.split()
        m = AGE.match(parts[1]) if len(parts) > 1 else None
        if not (m and any(m.groups())):
            continue
        h, mi, sec = (int(x or 0) for x in m.groups())
        if mi and not sec and not h and mi >= 5:   # coarse ages ("12m") are +-60 s: skip, a finer sample exists
            continue
        (nr if kind == "NodeNotReady" else rd).add(int((snap - (h * 3600 + mi * 60 + sec)) // 10) * 10)
    nrs, rds = sorted(nr), sorted(rd)
    flaps, last_end = [], -1
    for t in nrs:
        if t <= last_end:
            continue
        nxt = [r for r in rds if r >= t]
        end = nxt[0] if nxt else t + 999
        flaps.append((t, end - t)); last_end = end
    return flaps


def load(run):
    sm = glob.glob(run + "/*/summary_*.json")
    if not sm:
        return None
    t = list(json.load(open(sm[0]))["tests"].values())[0]
    db = json.load(open(glob.glob(run + "/*/*_db_samples.json")[0])) if glob.glob(run + "/*/*_db_samples.json") else []
    prom = json.load(open(glob.glob(run + "/*/*_prom_raw.json")[0])) if glob.glob(run + "/*/*_prom_raw.json") else {}
    probes = []
    for f in glob.glob(run + "/*/probe_*u.txt"):
        for l in open(f, errors="replace"):
            if l.startswith('{"type":"sample"'):
                try: probes.append(json.loads(l))
                except Exception: pass
    return t, db, prom, probes

def med(v):
    return statistics.median(v) if v else None

def analyze(run):
    L = load(run)
    if not L:
        return None
    t, db, prom, probes = L
    fails, ready = runner_events(run)
    flaps = node_flaps(run)
    lpa = t.get("workload") == "agentsession" or "lpa" in run
    atel = [(int(a), float(b)) for s in prom.get("running_atelet", []) for a, b in s["values"]]
    rows = []
    for s in t["steps"]:
        a, b = s["window"]
        users = s.get("users_target")
        sf = sum(1 for x in fails if a <= x < b)
        rd = sum(1 for x in ready if a <= x < b)
        denom = max(users or 1, rd)
        dbv = med([x.get("running_assigned") for x in db if a <= x["t"] < b and x.get("running_assigned") is not None])
        exe = [p["procs_exe"] for p in probes if a <= p["t"] < b and "procs_exe" in p]
        live = med(exe) if exe else med([v for tt, v in atel if a <= tt < b])
        src = "procs" if exe else "atelet"
        gap = ((dbv - live) / dbv) if (dbv and live is not None) else None
        fl = [d for (x, d) in flaps if a <= x < b]
        nrc = len(fl); nrmax = max(fl) if fl else 0
        slope = None
        if lpa:
            mem = sorted((p["t"], p["mem_MemAvailable_kb"] / 1048576) for p in probes
                         if b - 600 <= p["t"] < b and "mem_MemAvailable_kb" in p)
            if len(mem) >= 4:
                (t0, m0), (t1, m1) = mem[0], mem[-1]
                slope = (m1 - m0) / max(1, (t1 - t0) / 60)
        rf = s.get("fail_rate") or 0.0
        # gap (live vs DB) only judges non-churn workloads (LPA): in sweperf, actors churn every task and the
        # DB counts actors still starting, so the gap there is startup backlog, not dead sandboxes.
        base_ok = (sf / denom < 0.005) and (rf < 0.01) and (not lpa or gap is None or abs(gap) < 0.02)
        hard_ok = base_ok and nrmax <= 60          # relaxed: only NotReady > 60 s fails
        strict_ok = base_ok and nrc == 0           # strict: any flap fails
        steady = (not lpa) or (slope is not None and slope > -0.5)
        ok = hard_ok
        rows.append(dict(strict=strict_ok, nrmax=nrmax, steady=steady, lpa=lpa, users=users, sf=sf, sfp=100 * sf / denom, rf=100 * rf, live=live, src=src, db=dbv, gap=None if gap is None else 100 * gap,
                         nr=nrc, slope=slope, clean=ok, cpu=s.get("node_cpu_cores_avg"), mem=s.get("node_mem_gib_max"),
                         cyc=s.get("cycles_per_s"), res=s.get("resumes_per_s"), steady_missing=(lpa and slope is None)))
    return rows

def fmt(v, p=0, w=6):
    return ("%*.*f" % (w, p, v)) if isinstance(v, (int, float)) else " " * (w - 1) + "-"

out = io.StringIO()
cells = {}
for run in sorted(set(r for g in ARGS for r in glob.glob(g))):
    rows = analyze(run)
    name = os.path.basename(run)
    if rows is None:
        out.write(f"\n### {name}\n(no summary yet)\n"); continue
    out.write(f"\n### {name}\n\n| step | clean | start-fail | req-fail % | live | DB running | gap % | NotReady flaps (longest) | mem slope GiB/min | CPU | cycles/s | resumes/s |\n|---:|:-:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
    first_fail = None; last_clean = None; s_ff = None; s_lc = None
    for r in rows:
        out.write(f"| {r['users']} | {('✅' if r['steady'] else '⚠️') if r['clean'] else '❌'} | {r['sf']} ({r['sfp']:.1f}%) | {r['rf']:.2f} | {fmt(r['live'])} ({r['src']}) | {fmt(r['db'])} | {fmt(r['gap'],1)} | {r['nr']} ({r['nrmax']}s) | {fmt(r['slope'],2)} | {fmt(r['cpu'])} | {fmt(r['cyc'],1)} | {fmt(r['res'],1)} |\n")
        if r["clean"] and first_fail is None: last_clean = r
        if not r["clean"] and first_fail is None: first_fail = r
        if r["strict"] and s_ff is None: s_lc = r
        if not r["strict"] and s_ff is None: s_ff = r
    cell = re.sub(r"^[a-z0-9]+_(v2m?|opt)-", "", name).rsplit("-", 1)[0]
    cell = re.sub(r"_(us|eu|z3)$", "", cell)
    cells.setdefault(cell, []).append((name, last_clean, first_fail, s_lc))

out.write("\n## Per-cell summary (highest clean step before the first failing step)\n\n✅ clean · ⚠️ clean but memory still falling > 0.5 GiB/min in the last 10 min (not steady) · ❌ failed\n\n| cell | node | highest clean | steady there? | live there | first failing | why it failed | strict (any flap fails): highest clean |\n|---|---|---:|:-:|---:|---:|---|---:|\n")
for cell, lst in sorted(cells.items()):
    for name, lc, ff, slc in lst:
        why = ""
        if ff:
            w = []
            if ff["sfp"] >= 0.5: w.append(f"start-fail {ff['sfp']:.1f}%")
            if ff["rf"] >= 1: w.append(f"req-fail {ff['rf']:.1f}%")
            if ff["lpa"] and ff["gap"] is not None and abs(ff["gap"]) >= 2: w.append(f"live/DB gap {ff['gap']:.1f}% (dead sandboxes)")
            if ff["nrmax"] > 60: w.append(f"NotReady {ff['nrmax']}s")
            why = ", ".join(w)
        node = name.split("_")[0]
        out.write(f"| {cell} | {node} | {lc['users'] if lc else '—'} | {('yes' if lc['steady'] else 'no') if lc else '—'} | {fmt(lc['live']) if lc else '—'} | {ff['users'] if ff else 'none'} | {why} | {slc['users'] if slc else '—'} |\n")
txt = out.getvalue()
if OUT:
    open(OUT, "w").write(txt)
print(txt)
