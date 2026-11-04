#!/usr/bin/env python3
"""Run a queue of bench_driver experiments on one cluster, back to back, with worker logs + node dumps captured.

usage: campaign.py QUEUE_NAME   (queues defined in QUEUES below)
Status: scratch/CAMPAIGN_<QUEUE>.md ; results: ~/bench-runs/results/<run_id>/ ; logs: <results>/<run_id>/logs/
Never resizes or deletes node pools / reservations (nodes stay up; ask before removing).
"""
import datetime
import json, os
import pathlib
import subprocess
import sys
import threading
import time

HOME = pathlib.Path.home()
DRIVER = HOME / "bench-runs/driver"
RESULTS = HOME / "bench-runs/results"
SCRATCH = pathlib.Path("/usr/local/google/home/pmandewalkar/.gemini/jetski/brain/1a47f60a-167b-4100-8923-3e98e865d23a/scratch")
IMG_BASE = "gcr.io/pmandewalkar-gke-dev/ate-images/locust-test:59a58a5fb2c0e7958722ac4c4bc609fe7f16ddf8"
IMG_BUSY = "gcr.io/pmandewalkar-gke-dev/ate-images/locust-test:408fb70168d75027e1b709ef7b8461d4a9273e34"  # + busy-burn script
IMG_FIX = "gcr.io/pmandewalkar-gke-dev/ate-images/locust-test:c271800fc78a401f4f742d9937727a08005806e9"  # none + busy-burn + no-leak delete + liveness env
IMG_FIX2 = "gcr.io/pmandewalkar-gke-dev/ate-images/locust-test:7bfc72c67f3d34c8d5cf4895227f6d40b647350a"  # IMG_FIX + latency buckets up to ~35 min
IMG_NONE = "gcr.io/pmandewalkar-gke-dev/ate-images/locust-test:865bc3771beb154dfe6e536d8520001842323e4e"  # sweperf lifecycle none
PROBE_IMAGE = "gcr.io/pmandewalkar-gke-dev/ate-images/busybox:1.36"

CONTEXTS = {
    "bm_gvisor": "gke_pmandewalkar-gke-dev_us-central1-c_substrate-poc",
    "bm_microvm": "gke_pmandewalkar-gke-dev_us-central1-c_substrate-poc",
    "bm_gvisor_eu": "gke_pmandewalkar-gke-dev_europe-west1-b_substrate-eu",
    "bm_microvm_eu": "gke_pmandewalkar-gke-dev_europe-west1-b_substrate-eu",
    "bm_gvisor_z3": "gke_pmandewalkar-gke-dev_europe-west4-b_substrate-z3",
    "bm_microvm_z3": "gke_pmandewalkar-gke-dev_europe-west4-b_substrate-z3",
}


def with_pre(item, flags=(), node_io=False, numa_pin=False, sys_protect=False, halt_poll_ns=None):
    """Set prototype file flags (exactly these on, all others off), node I/O tuning and NUMA pinning before the run."""
    item = dict(item)
    item["pre"] = dict(flags=list(flags), node_io=node_io, numa_pin=numa_pin, sys_protect=sys_protect, halt_poll_ns=halt_poll_ns)
    return item


def autosize(top, cpu_cores, mem_gib, workers):
    """Workers and node-level overcommit factors so booking and --max-actors never bind below top (+25%)."""
    need = int(top * 1.25)
    w = max(workers, -(-need // 1000), -(-int(need * cpu_cores) // 999))
    oc = w * 999 / 191.45            # per-worker CPU limit lands on the 999-core clamp
    ocm = max(1.0, need * mem_gib / 486.0 * 1.1)  # node memory booking >= need x template memory
    return w, round(oc, 1), round(ocm, 1)


def sweperf(label, case, density, think, workers, oc, ocm, lifecycle, image, hold=240, spawn=None, skip=False, size_top=None,
            runner_env=""):
    top = size_top or max(int(x) for x in density.split(","))
    workers, oc, ocm = autosize(top, 2.0, 4.0, workers)  # sweperf template: 2 CPU / 4 GiB
    a = [case, "--workload", "sweperf", "--density", density, "--think", think,
         "--density-hold", str(hold), "--knee-label", label, "--workers", str(workers),
         "--overcommit", str(oc), "--overcommit-mem", str(ocm), "--lifecycle-mode", lifecycle,
         "--image", image, "--density-probe"]
    if spawn:
        a += ["--density-spawn", str(spawn)]
    if runner_env:
        a += ["--runner-env", runner_env]
    if skip:
        a += ["--skip-deploy"]
    return dict(label=label, case=case, args=a)


def lpa(label, case, users, workers, oc, ocm, hold=240, spawn=10, final_hold=0, skip=False, mem="512Mi", cpu="250m",
        script="personal-assistant-light", think_scale="0.053", image=None, abort="0.5"):
    top = max(int(x) for x in users.split(","))
    cpu_c = float(cpu[:-1]) / 1000 if cpu.endswith("m") else float(cpu)
    mem_g = float(mem[:-2]) / 1024 if mem.endswith("Mi") else float(mem[:-2])
    workers, oc, ocm = autosize(top, cpu_c, mem_g, workers)
    a = [case, "--workload", "agentsession", "--agentsession-users", users, "--knee-label", label,
         "--workers", str(workers), "--overcommit", str(oc), "--overcommit-mem", str(ocm), "--image", image or IMG_BASE,
         "--agentsession-script", script, "--agentsession-think-scale", think_scale,
         "--agentsession-phase-in", "60", "--agentsession-spawn", str(spawn), "--agentsession-hold", str(hold),
         "--agentsession-final-hold", str(final_hold), "--lifecycle-mode", "none", "--actor-cpu", cpu,
         "--actor-memory", mem, "--abort-fail-rate", abort, "--density-probe"]
    if skip:
        a += ["--skip-deploy"]
    return dict(label=label, case=case, args=a)


Z3 = "bm_gvisor_z3"
Z3M = "bm_microvm_z3"
QUEUES = {
    # Tuning screen on the third node (substrate-z3, branch density-proto). Short ramps; compare per-actor slopes.
    "z3_screen": [
        # gVisor LPA, one cgroup per actor vs two (ABA to control for kernel memory left over between runs).
        with_pre(lpa("z3_gv_lpa_base", Z3, "1500,3000", 8, 48, 10, hold=180), flags=()),
        with_pre(lpa("z3_gv_lpa_nocg", Z3, "1500,3000", 8, 48, 10, hold=180, skip=True), flags=("gvisor-no-app-cgroup",)),
        with_pre(lpa("z3_gv_lpa_base2", Z3, "1500,3000", 8, 48, 10, hold=180, skip=True), flags=()),
        # microVM LPA, virtiofsd cache auto vs never.
        with_pre(lpa("z3_mv_lpa_base", Z3M, "1000,2000", 4, 48, 10, hold=180), flags=()),
        with_pre(lpa("z3_mv_lpa_cachenever", Z3M, "1000,2000", 4, 48, 10, hold=180, skip=True), flags=("virtiofsd-cache-never",)),
    ],
    # microVM on the EU node. sweperf template = 2 CPU / 4 GiB; booking lifted on both CPU and memory.
    "eu_microvm": [
        # Active: suspend between cycles, realistic think (mean 8 s).
        sweperf("mv_active", "bm_microvm_eu", "100,200,300,400", "2,14", 1, 4, 4, "async", IMG_BASE),
        # Busy running: never suspend, think 0.5-2 s -> actors execute almost continuously.
        sweperf("mv_busy", "bm_microvm_eu", "50,100,150,200,250,300", "0.5,2", 1, 4, 4, "none", IMG_NONE, skip=True),
        # Running, mostly waiting: never suspend, think 30-90 s.
        sweperf("mv_resident", "bm_microvm_eu", "300,600,900,1200,1500,1800", "30,90", 2, 8, 8, "none", IMG_NONE, spawn=5),
        # LPA check.
        lpa("mv_lpa", "bm_microvm_eu", "1000,1500,2000,2500,3000", 4, 48, 10),
    ],
    # gVisor active (suspend/resume) limit: is ~30 resumes/s per worker or per node, and does node I/O tuning move it?
    # Exp 2c (1 worker, default node, booking lifted) flattened at ~30/s with resume p50 2.5-7 s.
    "z3_active": [
        with_pre(sweperf("z3_gv_active_w8", Z3, "250,350,450,550,700", "2,14", 8, 32, 32, "async", IMG_BASE), flags=()),
        with_pre(sweperf("z3_gv_active_w8_io", Z3, "250,350,450,550,700", "2,14", 8, 32, 32, "async", IMG_BASE, skip=True),
                 flags=(), node_io=True),
        with_pre(sweperf("z3_gv_active_w1_io", Z3, "150,250,350,450,550", "2,14", 1, 4, 4, "async", IMG_BASE),
                 flags=(), node_io=True),
    ],
    # gVisor active: one cgroup per actor on the swap path (fewer cgroup mkdir/rmdir/attach per swap).
    # Node dumps now carry D-state wait channels + kernel stacks.
    "z3_active2": [
        with_pre(sweperf("z3_gv_active_w8_io_nocg", Z3, "250,350,450,550", "2,14", 8, 32, 32, "async", IMG_BASE, hold=420),
                 flags=("gvisor-no-app-cgroup",), node_io=True),
        with_pre(sweperf("z3_gv_active_w8_io_base", Z3, "250,350,450,550", "2,14", 8, 32, 32, "async", IMG_BASE, hold=420, skip=True),
                 flags=(), node_io=True),
    ],
    # BUSY HARD LIMITS: never suspend, think 0.5-2 s, push far past the latency knee until actors fail/die/refuse
    # or the node falls over. 8 workers x 999 cores (clamped) / 2 CPU = ~3,992 slots; no latency stop rule.
    "us_busy_hard": [
        sweperf("gv_busy_hard", "bm_gvisor", "300,600,1000,1500,2000,3000", "0.5,2", 8, 42, 64, "none", IMG_NONE, spawn=5),
    ],
    "z3_busy_hard": [
        with_pre(sweperf("z3_mv_busy_hard", Z3M, "300,600,1000,1500,2000,3000", "0.5,2", 8, 42, 64, "none", IMG_NONE, spawn=3),
                 flags=(), node_io=True),
    ],
    # microVM busy: NUMA-pinned workers vs unpinned (host-kernel overhead + idle-with-backlog). Fresh deploy each, 8 workers.
    "eu_numa": [
        with_pre(sweperf("mv_busy_nopin", "bm_microvm_eu", "150,250,350", "0.5,2", 8, 42, 64, "none", IMG_NONE, hold=360, spawn=3),
                 node_io=True),
        with_pre(sweperf("mv_busy_pin", "bm_microvm_eu", "150,250,350", "0.5,2", 8, 42, 64, "none", IMG_NONE, hold=360, spawn=3),
                 node_io=True, numa_pin=True),
    ],
    # HARD BASELINES (no density tuning; infrastructure caps lifted).
    "eu_baselines": [
        # microVM LPA to failure (3,000 was clean with 70 GiB free).
        lpa("mv_lpa_fail", "bm_microvm_eu", "3000,3400,3800,4200", 5, 48, 10, hold=300, spawn=8),
        # microVM dormant: create -> first cycle -> suspend -> think 20,000-30,000 s (never wakes during the run).
        # Suspended actors hold no worker slot, so workers are sized for the few in-flight cycles only.
        sweperf("mv_dormant", "bm_microvm_eu", "2000,4000,6000,8000,10000", "20000,30000", 4, 0, 0, "async", IMG_BASE,
                hold=240, spawn=10, size_top=1500),
    ],
    # BUSY HARD LIMIT without churn: actors created once and burn one core continuously (never suspended, never recreated).
    "us_busyburn": [
        lpa("gv_busyburn", "bm_gvisor", "200,400,800,1200,1600,2400,3200", 4, 0, 0, hold=300, spawn=5, cpu="1", mem="512Mi",
            script="busy-burn", think_scale="1.0", image=IMG_BUSY, abort="0.9"),
    ],
    "z3_busyburn": [
        with_pre(lpa("mv_busyburn", Z3M, "200,400,800,1200,1600,2400,3200", 4, 0, 0, hold=300, spawn=3, cpu="1", mem="512Mi",
                     script="busy-burn", think_scale="1.0", image=IMG_BUSY, abort="0.9"), flags=(), node_io=True),
    ],
    # sweperf mostly-waiting baselines again, harness fixed (no leaked deletes; 5 min startup budget).
    "us_resident_fix": [
        sweperf("gv_resident_fix", "bm_gvisor", "2000,3000,4000,5000,6000,8000", "30,90", 4, 0, 0, "none", IMG_FIX, spawn=5,
                runner_env="SWEPERF_LIVENESS_TIMEOUT=5m"),
    ],
    "eu_resident_fix": [
        sweperf("mv_resident_fix", "bm_microvm_eu", "600,900,1200,1500,1800", "30,90", 4, 0, 0, "none", IMG_FIX, spawn=3,
                runner_env="SWEPERF_LIVENESS_TIMEOUT=5m"),
    ],
    "eu_rest": [
        sweperf("mv_dormant", "bm_microvm_eu", "2000,4000,6000,8000,10000", "20000,30000", 4, 0, 0, "async", IMG_FIX,
                hold=240, spawn=10, size_top=1500, runner_env="SWEPERF_LIVENESS_TIMEOUT=5m"),
        sweperf("mv_resident_fix", "bm_microvm_eu", "600,900,1200,1500,1800", "30,90", 4, 0, 0, "none", IMG_FIX, spawn=3,
                runner_env="SWEPERF_LIVENESS_TIMEOUT=5m"),
    ],
    # microVM busy-burn continued past 3,200 (0.8-1.6% failures there; no hard break yet).
    "z3_busyburn2": [
        with_pre(lpa("mv_busyburn2", Z3M, "3200,4000,4800,5600", 4, 0, 0, hold=300, spawn=3, cpu="1", mem="512Mi",
                     script="busy-burn", think_scale="1.0", image=IMG_BUSY, abort="0.9"), flags=(), node_io=True),
    ],
    "z3_dormant": [
        sweperf("mv_dormant_z3", Z3M, "2000,4000,6000,8000,10000", "20000,30000", 4, 0, 0, "async", IMG_FIX,
                hold=240, spawn=10, size_top=1500, runner_env="SWEPERF_LIVENESS_TIMEOUT=5m"),
    ],
    "eu_gv_resident_fix": [
        sweperf("gv_resident_fix_eu", "bm_gvisor_eu", "2000,3000,4000,5000,6000,8000", "30,90", 4, 0, 0, "none", IMG_FIX, spawn=5,
                runner_env="SWEPERF_LIVENESS_TIMEOUT=5m"),
    ],
    # Busy hard limit with the node agents protected (system.slice cpu.weight 100 -> 1500): A/B vs the gv_busyburn baseline.
    "us_busyburn_protect": [
        with_pre(lpa("gv_busyburn_protect", "bm_gvisor", "800,1200,1600,2400,3200,4000", 4, 0, 0, hold=300, spawn=5, cpu="1",
                     mem="512Mi", script="busy-burn", think_scale="1.0", image=IMG_BUSY, abort="0.9"), sys_protect=True),
    ],
    # --- Optimizations vs hard baselines (Oct 28) ---
    # gVisor LPA to failure with one cgroup per actor (baseline: 5,600 clean, 6,200 broke within minutes).
    # Busy hard limit with sweperf (fixed harness): short think, never suspended, ramp to failure.
    "us_busy_sweperf": [
        with_pre(sweperf("gv_busy_sweperf", "bm_gvisor", "150,300,500,800,1200,1600,2000", "0.5,2", 4, 0, 0, "none",
                         IMG_FIX, spawn=5, runner_env="SWEPERF_LIVENESS_TIMEOUT=5m")),
    ],
    "eu_busy_sweperf": [
        with_pre(sweperf("mv_busy_sweperf", "bm_microvm_eu", "100,200,400,600,900,1200,1600", "0.5,2", 4, 0, 0, "none",
                         IMG_FIX, spawn=3, runner_env="SWEPERF_LIVENESS_TIMEOUT=5m")),
    ],
    # CPU optimization: runsc --platform=kvm (vs systrap baseline us_busy_sweperf, same settings).
    "z3_kvm_smoke": [
        with_pre(lpa("gv_kvm_smoke", Z3, "50", 2, 0, 0, hold=120, spawn=5), flags=("runsc-extra-global-flags=--platform=kvm",)),
    ],
    "z3_busy_sweperf_kvm": [
        with_pre(sweperf("gv_busy_sweperf_kvm", Z3, "150,300,500,800,1200,1600,2000", "0.5,2", 4, 0, 0, "none",
                         IMG_FIX, spawn=5, runner_env="SWEPERF_LIVENESS_TIMEOUT=5m"),
                 flags=("runsc-extra-global-flags=--platform=kvm",)),
    ],
    # Same-node control for the KVM A/B: systrap (default) on z3, identical settings.
    "z3_busy_sweperf_base": [
        with_pre(sweperf("gv_busy_sweperf_base_z3", Z3, "150,300,500,800,1200,1600,2000", "0.5,2", 4, 0, 0, "none",
                         IMG_FIX2, spawn=5, runner_env="SWEPERF_LIVENESS_TIMEOUT=5m"), flags=()),
    ],
    "z3_opt_nocg_lpa": [
        with_pre(lpa("gv_lpa_nocg_fail", Z3, "5600,6000,6400,6800", 8, 0, 0, hold=420, spawn=10), flags=("gvisor-no-app-cgroup",)),
    ],
    # microVM LPA to failure with virtiofsd cache=never (baseline: 3,400 clean, 3,800 broke).
    "eu_opt_cn_lpa": [
        with_pre(lpa("mv_lpa_cn_fail", "bm_microvm_eu", "3000,3400,3800,4200", 5, 0, 0, hold=300, spawn=8),
                 flags=("virtiofsd-cache-never",)),
    ],
    # gVisor LPA steady at 3,000 for 20 min: per-sandbox memory breakdown taken by hand during the hold.
    "us_memdecomp": [
        lpa("gv_lpa_memdecomp", "bm_gvisor", "3000", 4, 0, 0, hold=1200, spawn=10),
    ],
    "us_dormant": [
        sweperf("gv_dormant", "bm_gvisor", "5000,10000,20000,30000,40000", "20000,30000", 4, 0, 0, "async", IMG_BASE,
                hold=240, spawn=25, size_top=2000),
    ],
    # microVM active again, node I/O tuned (Nishanth PR 5 + favordynmods); the default-node run collapsed on writeback.
    "eu_microvm_io": [
        with_pre(sweperf("mv_active_io", "bm_microvm_eu", "100,200,300,400", "2,14", 1, 4, 4, "async", IMG_BASE, skip=True),
                 node_io=True),
    ],
    "eu_microvm2": [
        with_pre(sweperf("mv_active_io", "bm_microvm_eu", "100,200,300,400", "2,14", 1, 4, 4, "async", IMG_BASE), node_io=True),
        # Busy diagnostic: why node CPU flattens at ~101 of 192 cores (node dumps now carry /proc/stat twice, 5 s apart).
        with_pre(sweperf("mv_busy_diag", "bm_microvm_eu", "250", "0.5,2", 1, 4, 4, "none", IMG_NONE, hold=900), node_io=True),
    ],
    # US follow-up: force the LPA memory cliff with a long hold and node-side log capture.
    "us_gvisor2": [
        lpa("gv_lpa_cliff_long", "bm_gvisor", "6200", 8, 48, 10, hold=900, spawn=10),
    ],
    # EU round 3 (density-proto build; flags default off). Disk raised to 2,400 MB/s / 40k IOPS at 20:56.
    "eu_microvm3": [
        # Active, tuned node + faster disk: does the ~6 swaps/s disk limit (750 MB/s at 6k IOPS) move?
        with_pre(sweperf("mv_active_disk", "bm_microvm_eu", "100,200,300,400", "2,14", 1, 4, 4, "async", IMG_BASE), node_io=True),
        # Same + virtiofsd cache=never (smaller snapshots).
        with_pre(sweperf("mv_active_disk_cn", "bm_microvm_eu", "100,200,300,400", "2,14", 1, 4, 4, "async", IMG_BASE, skip=True),
                 flags=("virtiofsd-cache-never",), node_io=True),
        # Mostly waiting, booking really lifted: 8 workers x 999 cores (clamped) = ~3,990 slots at 2 CPU.
        with_pre(sweperf("mv_resident8", "bm_microvm_eu", "800,1600,2400,3200", "30,90", 8, 42, 32, "none", IMG_NONE, spawn=5),
                 node_io=True),
    ],
    # US round 3: gVisor sweperf mostly waiting with booking really lifted (16 workers x 999 cores = ~7,990 slots).
    "us_gvisor3": [
        sweperf("gv_resident16", "bm_gvisor", "3000,5000,7000,9000,11000", "30,90", 16, 84, 64, "none", IMG_NONE, spawn=10),
    ],
    # gVisor on the US node, after day_runs finishes.
    "us_gvisor": [
        # Re-run the LPA memory cliff with log capture that survives stream loss (node-side greps of worker logs).
        lpa("gv_lpa_cliff", "bm_gvisor", "5600,6200", 8, 48, 10, hold=300, spawn=10),
        sweperf("gv_busy", "bm_gvisor", "50,100,150,200,250,300", "0.5,2", 1, 4, 4, "none", IMG_NONE),
        sweperf("gv_resident", "bm_gvisor", "1000,2000,3000,4000,5000,6000", "30,90", 8, 32, 32, "none", IMG_NONE, spawn=10),
    ],
}


class Capture:
    """Streams worker logs, snapshots events/pods, dumps node dmesg/journal/meminfo/slabinfo every 10 min."""

    def __init__(self, ctx, logdir):
        self.K = ["kubectl", "--context", ctx]
        self.logdir = logdir
        self.stop = threading.Event()
        self.streams = {}
        # only stream runner pods created after this capture started (old completed jobs linger)
        self.t0 = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.threads = [threading.Thread(target=self._collect, daemon=True),
                        threading.Thread(target=self._dumps, daemon=True)]

    def sh(self, cmd, timeout=300, **kw):
        return subprocess.run(cmd, text=True, capture_output=True, timeout=timeout, **kw)

    def start(self):
        self.logdir.mkdir(parents=True, exist_ok=True)
        for t in self.threads:
            t.start()
        self.node_dump("start")

    def finish(self):
        self.node_dump("end")
        self.stop.set()
        for t in self.threads:
            t.join(timeout=60)
        for p, f in self.streams.values():
            p.terminate()
            f.close()

    def _collect(self):
        last = 0
        while not self.stop.is_set():
            try:
                # worker pods + load-generator job pods (runner logs: kubelet rotation keeps only the tail)
                pods = []
                for ns_, pfx_, tag_ in (("benchmark-workloads", ("benchmark-worker",), "worker"),
                                        ("benchmarking", ("sw-", "as-"), "runner")):
                    for p in json.loads(self.sh(self.K + ["get", "pods", "-n", ns_, "-o", "json"]).stdout
                                        or '{"items":[]}')["items"]:
                        if p["metadata"]["name"].startswith(pfx_) and p["metadata"].get("creationTimestamp", "") >= self.t0:
                            pods.append((ns_, tag_, p))
                        elif tag_ == "worker" and p["metadata"]["name"].startswith(pfx_):
                            pods.append((ns_, tag_, p))
                for ns, tag, p in pods:
                    n, uid = p["metadata"]["name"], p["metadata"]["uid"][:8]
                    key = f"{n}-{uid}"
                    if key in self.streams:
                        proc, fh = self.streams[key]
                        if proc.poll() is None:
                            continue
                        fh.close()  # stream ended: restart from 2 min back (dupes are fine)
                        since = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
                        f = open(self.logdir / f"{tag}_{key}.log", "a")
                        f.write(f"\n### stream restarted, since {since}\n")
                        self.streams[key] = (subprocess.Popen(
                            self.K + ["logs", "-f", "-n", ns, n, "--all-containers", "--timestamps",
                                      "--prefix", "--max-log-requests", "20", f"--since-time={since}"],
                            stdout=f, stderr=subprocess.STDOUT), f)
                        continue
                    if p["status"].get("phase") not in ("Running", "Succeeded", "Failed"):
                        continue
                    f = open(self.logdir / f"{tag}_{key}.log", "a")
                    self.streams[key] = (subprocess.Popen(
                        self.K + ["logs", "-f", "-n", ns, n, "--all-containers", "--timestamps",
                                  "--prefix", "--max-log-requests", "20"], stdout=f, stderr=subprocess.STDOUT), f)
                if time.time() - last > 120:
                    last = time.time()
                    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    with open(self.logdir / "events.txt", "a") as f:
                        f.write(f"\n### {now}\n" + self.sh(self.K + ["get", "events", "-A", "--sort-by=.lastTimestamp"]).stdout[-20000:])
                    with open(self.logdir / "pods.txt", "a") as f:
                        f.write(f"\n### {now}\n" + self.sh(self.K + ["get", "pods", "-A", "-o", "wide"]).stdout)
            except Exception as ex:
                print(f"capture error {ex!r}", flush=True)
            self.stop.wait(30)

    def _dumps(self):
        i = 0
        while not self.stop.wait(600):
            i += 1
            self.node_dump(f"t{i:02d}")

    def node_dump(self, tag):
        try:
            out = self.sh(self.K + ["get", "nodes", "-l", "bench-node=c3-metal", "-o", "jsonpath={.items[0].metadata.name}"]).stdout.strip()
            if not out:
                return
            name = f"nodedump-{tag}-{int(time.time()) % 100000}"
            pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": "benchmarking"},
                   "spec": {"nodeName": out, "hostPID": True, "restartPolicy": "Never", "tolerations": [{"operator": "Exists"}],
                            "containers": [{"name": "d", "image": PROBE_IMAGE, "securityContext": {"privileged": True},
                                            "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}},
                                            "command": ["nsenter", "-t", "1", "-m", "-u", "-i", "-n", "--", "sh", "-c",
                                                        "echo '### dmesg'; dmesg -T | tail -n 5000; "
                                                        "echo '### kubelet'; journalctl -u kubelet --since '-15min' --no-pager | tail -n 5000; "
                                                        "echo '### containerd'; journalctl -u containerd --since '-15min' --no-pager | tail -n 3000; "
                                                        "echo '### meminfo'; cat /proc/meminfo; "
                                                        "echo '### vmstat'; cat /proc/vmstat; echo '### stat'; cat /proc/stat; sleep 5; echo '### stat2'; cat /proc/stat; "
                                                        "echo '### slabinfo'; cat /proc/slabinfo; "
                                                        "echo '### dstate'; for i in 1 2 3 4 5; do for t in /proc/[0-9]*/task/[0-9]*; do read -r a b c rest 2>/dev/null < $t/stat; [ \"$c\" = D ] && echo \"$(cat $t/comm 2>/dev/null) $(cat $t/wchan 2>/dev/null)\"; done; sleep 1; done | sort | uniq -c | sort -rn | head -40; "
                                                        "echo '### dstacks'; n=0; for t in /proc/[0-9]*/task/[0-9]*; do read -r a b c rest 2>/dev/null < $t/stat; if [ \"$c\" = D ]; then echo \"== $(cat $t/comm 2>/dev/null)\"; head -12 $t/stack 2>/dev/null; n=$((n+1)); [ $n -ge 25 ] && break; fi; done; "
                                                        "echo '### worker-log-events'; for f in /var/log/pods/benchmark-workloads_benchmark-worker*/*/*; do "
                                                        "case $f in *.gz) c=zcat;; *) c=cat;; esac; $c $f 2>/dev/null | grep -vE 'Actor usage sampled|OpenTelemetry SDK error|About to run runsc|Restore timing|Wakeup probe reached|Handle RPC|no egress policy' | grep -iE 'warn|error|panic|fatal|died|crash|kill' | tail -n 3000; done"]}]}}
            self.sh(self.K + ["apply", "-f", "-"], input=json.dumps(pod))
            self.sh(self.K + ["wait", "-n", "benchmarking", f"pod/{name}", "--for=jsonpath={.status.phase}=Succeeded",
                              "--timeout=240s"], timeout=300)
            (self.logdir / f"node_{tag}.txt").write_text(self.sh(self.K + ["logs", "-n", "benchmarking", name]).stdout)
            self.sh(self.K + ["delete", "pod", "-n", "benchmarking", name, "--wait=false"])
        except Exception as ex:
            print(f"node_dump {tag} error {ex!r}", flush=True)



# ---- Baseline v2 (2026-10-28): frozen config (all clones @ 5e46c801 = density-proto switches OFF + bucket fix),
# image IMG_FIX2, fixed driver, default node (sysctl DS only). Fine ladders around known limits.
# Order: non-destructive cells first (rotated per node), node-killing microVM cells last.
LIVE = "SWEPERF_LIVENESS_TIMEOUT=5m"


def _v2(item):
    item = with_pre(item, flags=())
    item["guard"] = True
    return item


def v2_cells(gv, mv, sfx):
    sw = lambda lab, case, dens, think, lc, hold, spawn: _v2(sweperf(f"v2_{lab}_{sfx}", case, dens, think, 4, 0, 0, lc, IMG_FIX2,
                                                                      hold=hold, spawn=spawn, runner_env=LIVE))
    lp = lambda lab, case, users, hold: _v2(lpa(f"v2_{lab}_{sfx}", case, users, 4, 0, 0, hold=hold, spawn=10, image=IMG_FIX2))
    return dict(
        gv_busy=sw("gv_busy", gv, "300,700,1000,1150,1300,1450,1600,1800", "0.5,2", "none", 480, 5),
        gv_wait=sw("gv_wait", gv, "1500,2500,2900,3300,3700,4100,4500", "30,90", "none", 480, 5),
        gv_active=sw("gv_active", gv, "100,175,250,325,400,500", "2,14", "async", 480, 5),
        gv_lpa=lp("gv_lpa", gv, "3000,4800,5200,5600,6000,6400", 1200),
        mv_wait=sw("mv_wait", mv, "500,900,1050,1200,1350,1500,1700", "30,90", "none", 480, 3),
        mv_active=sw("mv_active", mv, "40,70,100,130,160,200", "2,14", "async", 480, 3),
        mv_busy=sw("mv_busy", mv, "300,600,750,900,1050,1200,1400", "0.5,2", "none", 480, 3),
        mv_lpa=lp("mv_lpa", mv, "2000,2800,3100,3400,3700,4000", 1200),
    )


def v2_queue(gv, mv, sfx, rot):
    c = v2_cells(gv, mv, sfx)
    head = ["gv_busy", "gv_wait", "gv_active", "gv_lpa", "mv_wait", "mv_active"]
    head = head[rot:] + head[:rot]
    return [c[k] for k in head + ["mv_busy", "mv_lpa"]]


QUEUES["us_v2"] = v2_queue("bm_gvisor", "bm_microvm", "us", 0)
QUEUES["eu_v2"] = v2_queue("bm_gvisor_eu", "bm_microvm_eu", "eu", 2)
QUEUES["z3_v2"] = v2_queue(Z3, Z3M, "z3", 4)
# v2 on current main (2026-10-28 21:30): origin/main 475f49b0 + proto switches (off) + buckets + atunnel fix = ce8f4626
QUEUES["us_v2m"] = v2_queue("bm_gvisor", "bm_microvm", "us", 0)
QUEUES["eu_v2m"] = v2_queue("bm_gvisor_eu", "bm_microvm_eu", "eu", 2)
QUEUES["z3_v2m"] = v2_queue(Z3, Z3M, "z3", 4)

# ---- Optimization A/Bs vs baseline v2m (same build ce8f4626, same image). n=3 (one per node).
def opt_queue(gv, mv, sfx):
    sw = lambda lab, case, dens, think, lc, hold, spawn, **pre: _ab(sweperf(f"ab_{lab}_{sfx}", case, dens, think, 4, 0, 0, lc, IMG_FIX2,
                                                                            hold=hold, spawn=spawn, runner_env=LIVE), **pre)
    lp = lambda lab, case, users, hold, **pre: _ab(lpa(f"ab_{lab}_{sfx}", case, users, 4, 0, 0, hold=hold, spawn=10, image=IMG_FIX2), **pre)
    return [
        # node-agent CPU protection (system.slice cpu.weight 1500) on gVisor busy: same ladder as v2
        sw("gv_busy_sysprot", gv, "300,700,1000,1150,1300,1450,1600,1800", "0.5,2", "none", 480, 5, sys_protect=True),
        # one cgroup per actor on gVisor LPA: v2 limit ~5,600 -> ladder extends past it
        lp("gv_lpa_nocg", gv, "3000,5200,5600,6000,6400,6800", 1200, flags=("gvisor-no-app-cgroup",)),
        # virtiofsd cache=never on microVM LPA (may lose the node: last)
        lp("mv_lpa_cachenever", mv, "2000,2800,3100,3400,3700,4000,4300", 1200, flags=("virtiofsd-cache-never",)),
    ]


def _ab(item, flags=(), sys_protect=False):
    item = with_pre(item, flags=flags, sys_protect=sys_protect)
    item["guard"] = True
    return item


QUEUES["us_opt"] = opt_queue("bm_gvisor", "bm_microvm", "us")
QUEUES["eu_opt"] = opt_queue("bm_gvisor_eu", "bm_microvm_eu", "eu")
QUEUES["z3_opt"] = opt_queue(Z3, Z3M, "z3")

# ---- gVisor nightly (google/gvisor#16486, systrap host-stall fix) vs v6: same-node pairs, bracket ladders.
def gvn_pair(gv, sfx, nightly_first):
    lp = lambda lab, ver: _gvver(lpa(f"gvn_lpa_{lab}_{sfx}", gv, "3000,5200,5600,6000,6400", 4, 0, 0, hold=1200, spawn=10, image=IMG_FIX2), ver)
    sw = lambda lab, ver: _gvver(sweperf(f"gvn_active_{lab}_{sfx}", gv, "100,250,400", "2,14", 4, 0, 0, "async", IMG_FIX2,
                                          hold=480, spawn=5, runner_env=LIVE), ver)
    a, b = (lp("nightly", "v7nightly"), lp("v6", None)), (sw("nightly", "v7nightly"), sw("v6", None))
    if not nightly_first:
        a, b = (a[1], a[0]), (b[1], b[0])
    return [a[0], a[1], b[0], b[1]]


def _gvver(item, ver):
    item = with_pre(item, flags=())
    item["guard"] = True
    if ver:
        item["env"] = {"BENCH_GVISOR_VERSION": ver}
    return item


QUEUES["us_gvn"] = gvn_pair("bm_gvisor", "us", False)
QUEUES["eu_gvn"] = gvn_pair("bm_gvisor_eu", "eu", True)
QUEUES["z3_gvn"] = gvn_pair(Z3, "z3", False)

# ---- microVM suspend/resume: why does the default node fail at 40-100? base vs upload budget vs budget+I/O tuning.
def mva_queue(mv, sfx, rot):
    def run(lab, budget=None, node_io=False):
        it = sweperf(f"mva_{lab}_{sfx}", mv, "40,70,100,150,200,300", "2,14", 4, 0, 0, "async", IMG_FIX2,
                     hold=480, spawn=3, runner_env=LIVE)
        if budget:
            it["args"] = it["args"] + ["--upload-budget", budget]
        it = with_pre(it, flags=(), node_io=node_io)
        it["guard"] = True
        return it
    v = [run("base"), run("budget64g", "64Gi"), run("budget64g_io", "64Gi", True)]
    return v[rot:] + v[:rot]


QUEUES["us_mva"] = mva_queue("bm_microvm", "us", 0)
QUEUES["eu_mva"] = mva_queue("bm_microvm_eu", "eu", 1)
QUEUES["z3_mva"] = mva_queue(Z3M, "z3", 2)

# ---- Novel opt A: control-path CPU priority under overload (prototype switches in build 'worker-cpu-weight', 'restore-cpu-boost').
# Variants per cell: base | worker leaf cpu.weight 10000 | worker leaf + restore-time actor boost (1000). Same-node, rotated order.
def cpuprio_queue(gv, mv, sfx, rot):
    def sw(cell, case, dens, think, spawn, lab, flags):
        it = sweperf(f"cp_{cell}_{lab}_{sfx}", case, dens, think, 4, 0, 0, "none", IMG_FIX2, hold=480, spawn=spawn, runner_env=LIVE)
        it = with_pre(it, flags=flags)
        it["guard"] = True
        return it
    V = [("base", ()), ("wleaf", ("worker-cpu-weight",)), ("wleaf_boost", ("worker-cpu-weight", "restore-cpu-boost"))]
    V = V[rot:] + V[:rot]
    out = []
    for lab, fl in V:
        out.append(sw("gvbusy", gv, "700,1150,1300,1450,1600,1800", "0.5,2", 5, lab, fl))
    for lab, fl in V:
        out.append(sw("gvwait", gv, "1500,2500,2900,3300,3700", "30,90", 5, lab, fl))
    MV = [("base", ()), ("wleaf", ("worker-cpu-weight",))]   # restore boost is gVisor-only in the prototype
    MV = MV[rot % 2:] + MV[:rot % 2]
    for lab, fl in MV:
        out.append(sw("mvbusy", mv, "300,600,750,900,1050,1200", "0.5,2", 3, lab, fl))
    return out


QUEUES["us_cp"] = cpuprio_queue("bm_gvisor", "bm_microvm", "us", 0)
QUEUES["eu_cp"] = cpuprio_queue("bm_gvisor_eu", "bm_microvm_eu", "eu", 1)
QUEUES["z3_cp"] = cpuprio_queue(Z3, Z3M, "z3", 2)

# ---- Lever 2: (a) CPU-per-start profile (gVisor busy, base) (b) KVM halt polling off for microVM. Same build, same image.
def lever2_queue(gv, mv, sfx, rot):
    def sw(lab, case, dens, think, spawn, hp=None, profile=False):
        it = sweperf(f"l2_{lab}_{sfx}", case, dens, think, 4, 0, 0, "none", IMG_FIX2, hold=480, spawn=spawn, runner_env=LIVE)
        it = with_pre(it, flags=(), halt_poll_ns=hp)
        it["guard"] = True
        if profile:
            it["profile"] = True
        return it
    out = [sw("gvbusy_prof", gv, "300,700,1150", "0.5,2", 5, profile=True)]
    ab = []
    for cell, dens in (("mvbusy", "300,600,750,900,1050,1200"), ("mvwait", "500,900,1050,1200,1350,1500,1700")):
        think = "0.5,2" if cell == "mvbusy" else "30,90"
        pair = [sw(f"{cell}_base", mv, dens, think, 3, profile=True), sw(f"{cell}_hp0", mv, dens, think, 3, hp=0, profile=True)]
        if rot % 2:
            pair.reverse()
        ab += pair
    return out + ab


QUEUES["us_l2"] = lever2_queue("bm_gvisor", "bm_microvm", "us", 0)
QUEUES["eu_l2"] = lever2_queue("bm_gvisor_eu", "bm_microvm_eu", "eu", 1)
QUEUES["z3_l2"] = lever2_queue(Z3, Z3M, "z3", 2)

# ---- Memory lever: worker-side memory admission (proto switch 'mem-admission' = headroom GiB). Same-node pairs, rotated.
# Expect: no silent sandbox deaths / node loss past the limit; new starts refused (counted as start failures) instead.
def memadm_queue(gv, mv, sfx, rot):
    def lp(lab, case, users, flags):
        it = lpa(f"ma_{lab}_{sfx}", case, users, 4, 0, 0, hold=1200, spawn=10, image=IMG_FIX2)
        it = with_pre(it, flags=flags)
        it["guard"] = True
        it["profile"] = True
        return it
    out = []
    for cell, case, dens in (("gvlpa", gv, "3000,5200,5600,6000,6400"), ("mvlpa", mv, "1600,2500,2800,3100,3400")):
        pair = [lp(f"{cell}_base", case, dens, ()), lp(f"{cell}_adm24", case, dens, ("mem-admission=24",))]
        if rot % 2:
            pair.reverse()
        out += pair
    return out


QUEUES["us_ma"] = memadm_queue("bm_gvisor", "bm_microvm", "us", 0)
QUEUES["eu_ma"] = memadm_queue("bm_gvisor_eu", "bm_microvm_eu", "eu", 1)
QUEUES["z3_ma"] = memadm_queue(Z3, Z3M, "z3", 2)

# ---- Diagnostics: (1) gVisor busy with worker pprof (why per-start CPU grows with load)
#                   (2) microVM busy with deep profiler (per-CPU, run queue, thread states: the ~100-core plateau)
def diag_queue(gv, mv, sfx):
    a = sweperf(f"dg_gvbusy_pprof_{sfx}", gv, "300,700,1150", "0.5,2", 4, 0, 0, "none", IMG_FIX2, hold=480, spawn=5, runner_env=LIVE)
    a = with_pre(a, flags=("worker-pprof",)); a["guard"] = True; a["profile"] = True; a["pprof"] = True
    b = sweperf(f"dg_mvbusy_deep_{sfx}", mv, "300,600,900", "0.5,2", 4, 0, 0, "none", IMG_FIX2, hold=480, spawn=3, runner_env=LIVE)
    b = with_pre(b, flags=("worker-pprof",)); b["guard"] = True; b["profile"] = "deep"; b["pprof"] = True
    return [a, b]


QUEUES["us_dg"] = diag_queue("bm_gvisor", "bm_microvm", "us")
QUEUES["eu_dg"] = diag_queue("bm_gvisor_eu", "bm_microvm_eu", "eu")
QUEUES["z3_dg"] = diag_queue(Z3, Z3M, "z3")

# ---- Option 4: microVM NUMA pinning A/B (4 workers cleanly mapped to 4 NUMA nodes)
def numa_queue(mv, sfx):
    def sw(lab, pin):
        it = sweperf(f"numa_{lab}_{sfx}", mv, "300,600,750,900", "0.5,2", 4, 0, 0, "none", IMG_FIX2, hold=300, spawn=3, runner_env=LIVE)
        it = with_pre(it, flags=(), numa_pin=pin)
        it["guard"] = True
        it["profile"] = True
        return it
    return [sw("mv_nopin", False), sw("mv_pin", True)]


QUEUES["z3_numa"] = numa_queue(Z3M, "z3")

BASE_SHA, PR3497_SHA = "4f5f12fa", "b7fdec8f"
PR3497_NL_SHA = "aa6840da"
HOME_BR = pathlib.Path.home() / "bench-runs"
CLONES = {"us": HOME_BR / "substrate-main", "eu": HOME_BR / "substrate-eu", "z3": HOME_BR / "substrate-z3"}
CLONE_RESTORE = {"us": ["checkout", "-q", "--detach", BASE_SHA], "eu": ["checkout", "-q", "--detach", BASE_SHA],
                 "z3": ["checkout", "-q", "baseline-v2-main"]}


def netlink_opt_queue(gv, sfx):
    def sw(cell, dens, think, lc, lab, sha):
        it = sweperf(f"nl_{cell}_{lab}_{sfx}", gv, dens, think, 4, 0, 0, lc, IMG_FIX2, hold=240, spawn=5, runner_env=LIVE)
        it = with_pre(it, flags=())
        it["guard"] = True
        it["profile"] = True
        it["git"] = (sfx, sha)
        return it
    act = [
        sw("gvactive", "100,175,250,325,400,500", "2,14", "async", "v1_single_netns", PR3497_SHA),
        sw("gvactive", "100,175,250,325,400,500", "2,14", "async", "v2_netlink_opt", PR3497_NL_SHA),
    ]
    return act


QUEUES["eu_nl"] = netlink_opt_queue("bm_gvisor_eu", "eu")
QUEUES["eu_nl_leg2"] = [
    with_pre(sweperf("nl_gvactive_v2_netlink_opt_eu", "bm_gvisor_eu", "100,175,250,325,400,500", "2,14", 4, 0, 0, "async", IMG_FIX2, hold=240, spawn=5, runner_env=LIVE), flags=())
]
QUEUES["eu_nl_leg2"][0]["guard"] = True
QUEUES["eu_nl_leg2"][0]["profile"] = True
QUEUES["eu_nl_leg2"][0]["git"] = ("eu", PR3497_NL_SHA)



def netns_queue(gv, sfx, rot):
    def sw(cell, dens, think, lc, lab, sha):
        it = sweperf(f"nn_{cell}_{lab}_{sfx}", gv, dens, think, 4, 0, 0, lc, IMG_FIX2, hold=480, spawn=5, runner_env=LIVE)
        it = with_pre(it, flags=())
        it["guard"] = True
        it["profile"] = True
        it["git"] = (sfx, sha)
        return it
    act = [sw("gvactive", "100,175,250,325,400,500", "2,14", "async", "base", BASE_SHA),
           sw("gvactive", "100,175,250,325,400,500", "2,14", "async", "pr3497", PR3497_SHA)]
    busy = [sw("gvbusy", "700,1150,1300,1450,1600,1800", "0.5,2", "none", "pr3497", PR3497_SHA),
            sw("gvbusy", "700,1150,1300,1450,1600,1800", "0.5,2", "none", "base", BASE_SHA)]
    if rot % 2:
        act.reverse(); busy.reverse()
    return act + busy


QUEUES["us_nn"] = netns_queue("bm_gvisor", "us", 0)
QUEUES["eu_nn"] = netns_queue("bm_gvisor_eu", "eu", 1)
QUEUES["z3_nn"] = netns_queue(Z3, "z3", 2)


def checkout(sfx, sha, log):
    d = CLONES[sfx]
    if subprocess.run(["git", "-C", str(d), "status", "--porcelain"], capture_output=True, text=True).stdout.strip():
        raise RuntimeError(f"{d} has local changes; refusing to switch builds")
    r = subprocess.run(["git", "-C", str(d), "checkout", "-q", "--detach", sha], capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"git checkout {sha} in {d} failed: {r.stderr.strip()}")
    head = subprocess.run(["git", "-C", str(d), "log", "-1", "--format=%h %s"], capture_output=True, text=True).stdout.strip()
    log(f"build: {d.name} @ {head}")


PROFILER = pathlib.Path(__file__).parent / "node_cpu_profiler.sh"
PROFILER2 = pathlib.Path(__file__).parent / "node_cpu_profiler2.sh"


def start_pprof_capture(ctx, outdir, log, every=300, seconds=30):
    """Every `every` s, pull a `seconds`-long CPU profile from each worker pod's pprof (127.0.0.1:6060 in the pod)."""
    stop = threading.Event()
    K = ["kubectl", "--context", ctx]

    def loop():
        outdir.mkdir(parents=True, exist_ok=True)
        stop.wait(240)  # let deploy + first step start
        port = 16060
        while not stop.is_set():
            pods = subprocess.run(K + ["get", "pods", "-n", "benchmark-workloads", "-o", "name"], text=True, capture_output=True).stdout.split()
            for pod in [p for p in pods if "benchmark-worker" in p][:3]:
                if stop.is_set():
                    break
                pf = subprocess.Popen(K + ["port-forward", "-n", "benchmark-workloads", pod, f"{port}:6060"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(3)
                ts = time.strftime("%H%M%S", time.gmtime())
                name = pod.split("/")[-1]
                subprocess.run(["curl", "-s", "-m", str(seconds + 20), "-o", str(outdir / f"{name}_{ts}.pb.gz"),
                                f"http://127.0.0.1:{port}/debug/pprof/profile?seconds={seconds}"], capture_output=True)
                pf.terminate()
                port += 1
            stop.wait(every)

    threading.Thread(target=loop, daemon=True).start()
    log("pprof: capture loop started")
    return stop


def start_profiler(ctx, log, kind=True):
    K = ["kubectl", "--context", ctx]
    node = subprocess.run(K + ["get", "nodes", "-l", "bench-node=c3-metal", "-o", "jsonpath={.items[0].metadata.name}"],
                          text=True, capture_output=True).stdout.strip()
    subprocess.run(K + ["-n", "kube-system", "delete", "cm", "bench-cpuprof", "--ignore-not-found"], capture_output=True)
    subprocess.run(K + ["-n", "kube-system", "create", "configmap", "bench-cpuprof", f"--from-file=p.sh={PROFILER2 if kind == 'deep' else PROFILER}"], capture_output=True)
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "bench-cpuprof", "namespace": "kube-system"},
           "spec": {"nodeName": node, "hostPID": True, "restartPolicy": "Never", "priorityClassName": "system-node-critical",
                    "tolerations": [{"operator": "Exists"}],
                    "volumes": [{"name": "s", "configMap": {"name": "bench-cpuprof"}}],
                    "containers": [{"name": "c", "image": PROBE_IMAGE, "securityContext": {"privileged": True},
                                    "env": [{"name": "DUR", "value": "21600"}, {"name": "INTERVAL", "value": "10"}],
                                    "volumeMounts": [{"name": "s", "mountPath": "/s"}],
                                    "command": ["sh", "/s/p.sh"]}]}}
    subprocess.run(K + ["delete", "pod", "-n", "kube-system", "bench-cpuprof", "--ignore-not-found", "--wait=true"], capture_output=True)
    subprocess.run(K + ["apply", "-f", "-"], input=json.dumps(pod), text=True, capture_output=True)
    log("profiler: started node CPU-by-cgroup sampler")
    return True


def stop_profiler(ctx, prof, out, log):
    K = ["kubectl", "--context", ctx]
    r = subprocess.run(K + ["logs", "-n", "kube-system", "bench-cpuprof"], text=True, capture_output=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(r.stdout)
    subprocess.run(K + ["delete", "pod", "-n", "kube-system", "bench-cpuprof", "--wait=false"], capture_output=True)
    log(f"profiler: saved {len(r.stdout.splitlines())} lines -> {out.name}")


def ensure_node_ready(ctx, log, wait_s=600):
    """Baseline v2: before each run, require the metal node Ready; if NotReady for wait_s, reset the VM
    (GKE auto-repair is disabled on these pools) and wait for it to come back."""
    K = ["kubectl", "--context", ctx]
    zone = ctx.split("_")[2]
    def ready():
        r = subprocess.run(K + ["get", "nodes", "-l", "bench-node=c3-metal", "-o",
                                "jsonpath={.items[0].metadata.name} {.items[0].status.conditions[?(@.type=='Ready')].status}"],
                           text=True, capture_output=True, timeout=60)
        parts = r.stdout.split()
        return (parts[0] if parts else ""), (len(parts) > 1 and parts[1] == "True")
    t0 = time.time()
    while time.time() - t0 < wait_s:
        node, ok = ready()
        if ok:
            return True
        time.sleep(30)
    node, ok = ready()
    if ok:
        return True
    log(f"!!! node {node} NotReady for {wait_s}s: resetting VM")
    subprocess.run(["gcloud", "compute", "instances", "reset", node, "--zone", zone, "--project", "pmandewalkar-gke-dev", "--quiet"],
                   text=True, capture_output=True, timeout=300)
    t1 = time.time()
    while time.time() - t1 < 1500:
        time.sleep(30)
        node, ok = ready()
        if ok:
            log(f"node {node} Ready again after reset; settling 240s")
            time.sleep(240)  # let old namespaces/pods clear (namespace-termination race)
            return True
    log(f"!!! node {node} still NotReady after reset")
    return False

def apply_pre(ctx, pre, log):
    K = ["kubectl", "--context", ctx]
    # "name" -> empty switch file; "name=value" -> file containing value (e.g. runsc-extra-global-flags=--platform=kvm)
    def _flag_cmd(f):
        if "=" in f:
            n, v = f.split("=", 1)
            return f"printf '%s\\n' '{v}' > /var/lib/ate/proto/{n};"
        return f"touch /var/lib/ate/proto/{f};"
    flags = " ".join(_flag_cmd(f) for f in pre["flags"])
    node = subprocess.run(K + ["get", "nodes", "-l", "bench-node=c3-metal", "-o", "jsonpath={.items[0].metadata.name}"],
                          text=True, capture_output=True).stdout.strip()
    name = f"protoflags-{int(time.time()) % 100000}"
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": "kube-system"},
           "spec": {"nodeName": node, "hostPID": True, "restartPolicy": "Never", "priorityClassName": "system-node-critical", "tolerations": [{"operator": "Exists"}],
                    "containers": [{"name": "f", "image": PROBE_IMAGE, "securityContext": {"privileged": True},
                                    "command": ["nsenter", "-t", "1", "-m", "--", "sh", "-c",
                                                f"rm -rf /var/lib/ate/proto; mkdir -p /var/lib/ate/proto; {flags} "
                                                f"echo {200000 if pre.get('halt_poll_ns') is None else pre.get('halt_poll_ns')} > /sys/module/kvm/parameters/halt_poll_ns 2>/dev/null; "
                                                f"ls /var/lib/ate/proto"]}]}}
    subprocess.run(K + ["apply", "-f", "-"], input=json.dumps(pod), text=True, capture_output=True)
    subprocess.run(K + ["wait", "-n", "kube-system", f"pod/{name}", "--for=jsonpath={.status.phase}=Succeeded", "--timeout=180s"],
                   text=True, capture_output=True)
    got = subprocess.run(K + ["logs", "-n", "kube-system", name], text=True, capture_output=True).stdout.split()
    subprocess.run(K + ["delete", "pod", "-n", "kube-system", name, "--wait=false"], text=True, capture_output=True)
    if pre["node_io"]:
        subprocess.run(K + ["apply", "-f", str(DRIVER / "node_io_tuner_ds.yaml")], text=True, capture_output=True)
    else:
        r = subprocess.run(K + ["delete", "ds", "-n", "kube-system", "substrate-node-io-tuner", "--ignore-not-found"],
                           text=True, capture_output=True)
        if "deleted" in r.stdout:
            subprocess.run(K + ["delete", "pod", "-n", "kube-system", "node-io-revert", "--ignore-not-found"], capture_output=True)
            subprocess.run(K + ["apply", "-f", str(DRIVER / "node_io_revert.yaml")], text=True, capture_output=True)
    if pre.get("numa_pin"):
        subprocess.run(K + ["apply", "-f", str(DRIVER / "numa_pinner_ds.yaml")], text=True, capture_output=True)
    else:
        subprocess.run(K + ["delete", "ds", "-n", "kube-system", "substrate-numa-pinner", "--ignore-not-found"],
                       text=True, capture_output=True)
    if pre.get("sys_protect"):
        subprocess.run(K + ["apply", "-f", str(DRIVER / "system_cpu_protect_ds.yaml")], text=True, capture_output=True)
    else:
        subprocess.run(K + ["delete", "ds", "-n", "kube-system", "substrate-system-cpu-protect", "--ignore-not-found"],
                       text=True, capture_output=True)
        # weight is sticky after the DaemonSet goes away; restore the default
        subprocess.run(K + ["delete", "pod", "-n", "kube-system", "sysw-reset", "--ignore-not-found"], capture_output=True)
        node = subprocess.run(K + ["get", "nodes", "-l", "bench-node=c3-metal", "-o", "jsonpath={.items[0].metadata.name}"],
                              text=True, capture_output=True).stdout.strip()
        pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "sysw-reset", "namespace": "kube-system"},
               "spec": {"nodeName": node, "hostPID": True, "restartPolicy": "Never", "priorityClassName": "system-node-critical", "tolerations": [{"operator": "Exists"}],
                        "containers": [{"name": "r", "image": PROBE_IMAGE, "securityContext": {"privileged": True},
                                        "command": ["nsenter", "-t", "1", "-m", "--", "sh", "-c",
                                                    "echo 100 > /sys/fs/cgroup/system.slice/cpu.weight"]}]}}
        subprocess.run(K + ["apply", "-f", "-"], input=json.dumps(pod), text=True, capture_output=True)
    time.sleep(20)
    log(f"pre: numa_pin={bool(pre.get('numa_pin'))} sys_protect={bool(pre.get('sys_protect'))} halt_poll_ns={200000 if pre.get('halt_poll_ns') is None else pre.get('halt_poll_ns')}")
    log(f"pre: flags on node = {got} (wanted {pre['flags']}); node_io={'tuned' if pre['node_io'] else 'default'}")
    if sorted(got) != sorted(f.split("=", 1)[0] for f in pre["flags"]):
        raise RuntimeError(f"proto flags not applied: got {got}")


def main():
    qname = sys.argv[1]
    status = SCRATCH / f"CAMPAIGN_{qname}.md"
    lines = []

    def log(m):
        line = f"{datetime.datetime.now(datetime.timezone.utc):%H:%M:%S} {m}"
        print(line, flush=True)
        lines.append(line)
        status.write_text(f"# Campaign {qname}\n```\n" + "\n".join(lines[-60:]) + "\n```\n")

    used_git = set()
    for item in QUEUES[qname]:
        rid = f"{qname}-{item['label']}-{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%SZ}"
        rdir = RESULTS / rid
        rdir.mkdir(parents=True, exist_ok=True)
        cap = Capture(CONTEXTS[item["case"]], rdir / "logs")
        log(f"START {item['label']} -> {rid}")
        if item.get("guard") and not ensure_node_ready(CONTEXTS[item["case"]], log):
            log(f"!!! {item['label']} skipped: node not Ready")
            continue
        try:
            if item.get("git"):
                checkout(*item["git"], log)
                used_git.add(item["git"][0])
            if "pre" in item:
                apply_pre(CONTEXTS[item["case"]], item["pre"], log)
        except Exception as ex:
            log(f"!!! {item['label']} pre failed {ex!r}; skipping")
            continue
        cap.start()
        prof = start_profiler(CONTEXTS[item["case"]], log, item.get("profile")) if item.get("profile") else None
        pp = start_pprof_capture(CONTEXTS[item["case"]], rdir / "logs" / "pprof", log) if item.get("pprof") else None
        try:
            env = dict(os.environ, **item.get("env", {}))
            if item.get("env"):
                log(f"env: {item['env']}")
            r = subprocess.run(["python3", "bench_driver.py", *item["args"], "--run-id", rid], cwd=DRIVER,
                               text=True, capture_output=True, timeout=6 * 3600, env=env)
            (rdir / "console.txt").write_text(r.stdout[-300000:] + "\n--- stderr ---\n" + r.stderr[-100000:])
            log(f"END {item['label']} rc={r.returncode}")
        except Exception as ex:
            log(f"!!! {item['label']} error {ex!r}")
        finally:
            if pp:
                pp.set()
            if prof:
                stop_profiler(CONTEXTS[item["case"]], prof, rdir / "logs" / "cpu_profile.txt", log)
            cap.finish()
    for sfx in used_git:
        r = subprocess.run(["git", "-C", str(CLONES[sfx])] + CLONE_RESTORE[sfx], capture_output=True, text=True)
        log(f"build restored: {CLONES[sfx].name} rc={r.returncode} {r.stderr.strip()}")
    log("QUEUE DONE (nodes left running)")


if __name__ == "__main__":
    main()
