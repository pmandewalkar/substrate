#!/usr/bin/env python3
"""Unattended sweperf node-limit benchmark driver.

Runs one case (cluster + sandbox class + target node) end to end, using only
upstream main scripts from an isolated clone:
  teardown -> hack/install-ate.sh --deploy-ate-system --otlp-endpoint <meter>
  -> hack/install-microvm-deps.sh (microvm) -> benchmarking/workloads/deploy.sh
  -> runner Job (benchmarking/locust/runner.py, ladder shape) per test.

Tests per case (definitions: Registered / Running / Not-Running / Active / Inactive):
  T1  closed-loop users ladder (think 0-0.5s)         -> max Active, max Running
  T2  10% duty cycle ladder (think = 9 x cycle time)  -> team-standard density point
  T2b long think (~30 min) ladder                     -> max Registered / Not-Running

No repo files are modified. Everything is written to RESULTS_ROOT/<run_id>/<case>/
and mirrored to GCS.

--workload agentsession (idle-active density): runs boomer's AgentSessionUser
(/app/tests/agentsession.py) with a script (default personal-assistant-light) against the
benchmark-workloads/glutton template sized by deploy.sh --actor-cpu/--actor-memory. With
--lifecycle-mode none each user holds exactly one RUNNING actor, so --agentsession-users is
an upward-only ladder of running-actor counts, each held --agentsession-hold seconds, plus
an optional --agentsession-final-hold at the top step (only kept if the top step was clean).
The ladder is stopped early (job deleted) when a step's failure rate exceeds --abort-fail-rate.
Templates: deploy.sh sources the clone's .ate-dev-env.sh (WORKLOAD_TEMPLATES=sweperf-...), so
the driver runs deploy.sh through a bash wrapper whose `source` re-exports
WORKLOAD_TEMPLATES=glutton after that file is read; the clone is not modified.
--density-probe swaps node_probe.sh for density_probe.sh (15 s time series over each step).
--dry-run renders the deploy commands, ladder and runner Job manifest without a cluster.
"""

import argparse
import datetime as dt
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import yaml  # PyYAML

HOME = Path.home()
RESULTS_ROOT = HOME / "bench-runs" / "results"
COMMIT = open(HOME / "bench-runs" / "locust_image.txt").read().strip().split(":")[-1]
LOCUST_IMAGE = open(HOME / "bench-runs" / "locust_image.txt").read().strip().split("=", 1)[1]
METER = "http://telemetry-meter.benchmarking.svc.cluster.local:4317"
BUCKET = "snapshot-substrate-test-pmandewalkar-gke-dev"
# WorkerPool deploy.sh creates; renamed benchmark-worker on main after #2254.
WORKER_POOL = "benchmark-worker"

CASES = {
    "bm_microvm": dict(
        context="gke_pmandewalkar-gke-dev_us-central1-c_substrate-poc",
        clone=HOME / "bench-runs" / "substrate-main",
        sandbox="microvm", node_label="bench-node=c3-metal", prom_port=19090,
        machine="c3-highcpu-192-metal"),
    "bm_gvisor": dict(
        context="gke_pmandewalkar-gke-dev_us-central1-c_substrate-poc",
        clone=HOME / "bench-runs" / "substrate-main",
        sandbox="gvisor", node_label="bench-node=c3-metal", prom_port=19090,
        machine="c3-highcpu-192-metal"),
    "bm_gvisor_eu": dict(
        context="gke_pmandewalkar-gke-dev_europe-west1-b_substrate-eu",
        clone=HOME / "bench-runs" / "substrate-eu",
        sandbox="gvisor", node_label="bench-node=c3-metal", prom_port=19092,
        machine="c3-highcpu-192-metal",
        bucket="snapshot-substrate-eu-pmandewalkar-gke-dev"),
    "bm_microvm_eu": dict(
        context="gke_pmandewalkar-gke-dev_europe-west1-b_substrate-eu",
        clone=HOME / "bench-runs" / "substrate-eu",
        sandbox="microvm", node_label="bench-node=c3-metal", prom_port=19093,
        machine="c3-highcpu-192-metal",
        bucket="snapshot-substrate-eu-pmandewalkar-gke-dev"),
    "bm_gvisor_z3": dict(
        context="gke_pmandewalkar-gke-dev_europe-west4-b_substrate-z3",
        clone=HOME / "bench-runs" / "substrate-z3",
        sandbox="gvisor", node_label="bench-node=c3-metal", prom_port=19094,
        machine="c3-highcpu-192-metal",
        bucket="snapshot-substrate-z3-pmandewalkar-gke-dev"),
    "bm_microvm_z3": dict(
        context="gke_pmandewalkar-gke-dev_europe-west4-b_substrate-z3",
        clone=HOME / "bench-runs" / "substrate-z3",
        sandbox="microvm", node_label="bench-node=c3-metal", prom_port=19095,
        machine="c3-highcpu-192-metal",
        bucket="snapshot-substrate-z3-pmandewalkar-gke-dev"),
    "vm_gvisor": dict(
        context="gke_pmandewalkar-gke-dev_us-central1-a_substrate-vm",
        clone=HOME / "bench-runs" / "substrate-vm-clone",
        sandbox="gvisor", node_label="bench-node=c3-vm", prom_port=19091,
        machine="c3-highcpu-176"),
}

# Ladders. Holds are >= 4 min (repo minimum is 3 min for 60s metric export).
T1_USERS = [25, 50, 100, 150, 200, 300, 400, 600, 800, 1000, 1200]
T2_USERS = [250, 500, 1000, 1500, 2000, 3000, 5000, 7500, 10000]  # unused; T2 is sized from T1
T2_MULT = [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0]
T2B_USERS = [500, 1000, 2500, 5000, 7500, 10000, 15000, 20000]
MEASURE_S = 240   # measurement window per step
SETTLE_S = 60     # settle after ramp before measuring
T2B_BUDGET_S = 150 * 60

LOG = None
RUNNER_ENV = {}
SWEPERF_TEMPLATE = ""  # --sweperf-template override passed to the runner
OVERCOMMIT_MEM = 0.0  # memory overcommit factor; 0 = same as OVERCOMMIT
UPLOAD_BUDGET = ""
WORKERS = 1
DENSITY_THINK = (1500.0, 2100.0)  # density mode: think time between a user's cycles (s)
DENSITY_HOLD = 0  # density mode: seconds to hold each step after its ramp (0 = default)
DENSITY_SPAWN = 0.0  # density mode: users added per second (0 = per-sandbox default)
OVERCOMMIT = 0.0  # >0: per-node CPU/mem overcommit emulated via worker container limits (split across workers)
PROBE_AT = []
PROBE_SECS = 150
PROBE_SCRIPT = HOME / "bench-runs" / "driver" / "node_probe.sh"
DENSITY_PROBE_SCRIPT = HOME / "bench-runs" / "driver" / "density_probe.sh"
DENSITY_PROBE = False      # --density-probe: density_probe.sh over each probed step instead of node_probe.sh
DENSITY_PROBE_INTERVAL = 15
PROBE_IMAGE = "gcr.io/pmandewalkar-gke-dev/ate-images/busybox:1.36"

# ---- workload selection (sweperf unchanged by default)
WORKLOAD = "sweperf"       # or "agentsession"
ACTOR_PREFIX = "sb-"       # benchmark actor name prefix: sweperf sb-<uuid>, agentsession agent-<uuid>
LIFECYCLE_MODE = ""        # passed to the runner only when set (agentsession: always, default none)
ACTOR_CPU = ""             # deploy.sh --actor-cpu (agentsession default 250m)
ACTOR_MEMORY = ""          # deploy.sh --actor-memory (agentsession default 512Mi)
AGENT = dict(script="personal-assistant-light", think_scale=0.053, phase_in=60.0, resume_retry_budget=None,
             spawn=5.0, hold=600, final_hold=0, templates="glutton")
ABORT_FAIL_RATE = 0.5      # agentsession: stop ramping when a step's failure rate (or placement shortfall) exceeds this
FINAL_HOLD_MAX_FAIL = 0.05  # agentsession: keep the final hold only if the top step's failure rate is below this
CLEAN_MAX_SHORTFALL = 0.02  # agentsession: a step is clean if >= 98% of users have a running actor
DRY_RUN = False
DRY_ALLOC = dict(cpu="191450m", memory="509607936Ki")  # assumed metal allocatable for --dry-run (~486 GiB)


def log(msg):
    line = f"[{dt.datetime.now(dt.timezone.utc).strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    if LOG:
        LOG.write(line + "\n")
        LOG.flush()


class Env:
    def __init__(self, case_name, out_dir):
        self.name = case_name
        self.c = CASES[case_name]
        self.out = out_dir
        self.clone = Path(self.c["clone"])
        self.kubeconfig = out_dir / "kubeconfig"
        raw = subprocess.check_output(
            ["kubectl", "config", "view", "--raw", "--minify", "--flatten",
             "--context", self.c["context"]], text=True)
        self.kubeconfig.write_text(raw)
        self.env = dict(os.environ, KUBECONFIG=str(self.kubeconfig))
        # Source the clone's .ate-dev-env.sh so scripts and we agree.
        out = subprocess.check_output(
            ["bash", "-c", "set -a; source .ate-dev-env.sh; env"],
            cwd=self.clone, env=self.env, text=True)
        for line in out.splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                self.env[k] = v
        self.env["KUBECONFIG"] = str(self.kubeconfig)
        assert self.env.get("KUBECTL_CONTEXT") == self.c["context"], "context pin mismatch"
        self.pf = None

    # ---- shell helpers ----
    def run(self, cmd, check=True, timeout=None, capture=False, input=None):
        log("$ " + (cmd if isinstance(cmd, str) else " ".join(map(str, cmd)))[:300])
        r = subprocess.run(cmd, cwd=self.clone, env=self.env, shell=isinstance(cmd, str),
                           text=True, timeout=timeout, input=input,
                           stdout=subprocess.PIPE if capture else LOG,
                           stderr=subprocess.STDOUT if not capture else subprocess.PIPE)
        if check and r.returncode != 0:
            raise RuntimeError(f"command failed ({r.returncode}): {cmd}\n{(r.stderr or '')[-2000:]}")
        return r

    def k(self, *args, check=True, capture=True, input=None):
        return self.run(["kubectl", *args], check=check, capture=capture, input=input)

    def ate(self, *args, check=True):
        return self.run([str(self.clone / "bin" / "kubectl-ate"), *args], check=check, capture=True)

    # ---- prometheus ----
    def prom_start(self):
        self.prom_stop()
        port = self.c["prom_port"]
        self.pf = subprocess.Popen(
            ["kubectl", "port-forward", "-n", "benchmarking", "svc/prometheus", f"{port}:9090"],
            env=self.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(30):
            try:
                urllib.request.urlopen(f"http://localhost:{port}/-/ready", timeout=2)
                return
            except Exception:
                time.sleep(1)
        raise RuntimeError("prometheus port-forward failed")

    def prom_stop(self):
        if self.pf:
            self.pf.terminate()
            self.pf = None

    def q(self, expr, t=None):
        params = {"query": expr}
        if t:
            params["time"] = str(t)
        for attempt in range(3):
            try:
                u = f"http://localhost:{self.c['prom_port']}/api/v1/query?" + urllib.parse.urlencode(params)
                return json.load(urllib.request.urlopen(u, timeout=30))["data"]["result"]
            except Exception:
                self.prom_start()
        return []

    def qv(self, expr, t=None):
        r = self.q(expr, t)
        try:
            v = float(r[0]["value"][1])
            return None if math.isnan(v) or math.isinf(v) else v
        except (IndexError, KeyError, ValueError):
            return None

    def qrange(self, expr, start, end, step=15):
        params = {"query": expr, "start": str(start), "end": str(end), "step": str(step)}
        for attempt in range(3):
            try:
                u = f"http://localhost:{self.c['prom_port']}/api/v1/query_range?" + urllib.parse.urlencode(params)
                return json.load(urllib.request.urlopen(u, timeout=60))["data"]["result"]
            except Exception:
                self.prom_start()
        return []


# ---------------------------------------------------------------- setup


def target_node(e):
    out = e.k("get", "nodes", "-l", e.c["node_label"], "-o", "jsonpath={.items[*].metadata.name}").stdout.split()
    if len(out) != 1:
        raise RuntimeError(f"expected exactly 1 node with {e.c['node_label']}, got {out}")
    return out[0]


def deploy_sh(*args):
    """benchmarking/workloads/deploy.sh argv. agentsession: run it through a bash wrapper that re-exports
    WORKLOAD_TEMPLATES (from BENCH_WORKLOAD_TEMPLATES) right after deploy.sh sources the clone's
    .ate-dev-env.sh, which pins WORKLOAD_TEMPLATES to the sweperf template. The clone is not modified."""
    if WORKLOAD != "agentsession":
        return ["benchmarking/workloads/deploy.sh", *args]
    wrapper = ('source() { builtin source "$@"; local rc=$?; '
               'if [[ "${1:-}" == .ate-dev-env.sh && -n "${BENCH_WORKLOAD_TEMPLATES:-}" ]]; then '
               'export WORKLOAD_TEMPLATES="$BENCH_WORKLOAD_TEMPLATES"; fi; return $rc; }; '
               'export -f source; exec benchmarking/workloads/deploy.sh "$@"')
    return ["bash", "-c", wrapper, "deploy.sh", *args]


def workload_deploy_args():
    """Extra deploy.sh args for the workload's ActorTemplate sizing."""
    a = []
    if ACTOR_CPU:
        a += ["--actor-cpu", ACTOR_CPU]
    if ACTOR_MEMORY:
        a += ["--actor-memory", ACTOR_MEMORY]
    return a


def teardown(e):
    if WORKLOAD == "agentsession":
        e.env["BENCH_WORKLOAD_TEMPLATES"] = AGENT["templates"]
    e.run(deploy_sh("--delete"), check=False)
    e.run(["hack/install-microvm-deps.sh", "--delete"], check=False)
    e.run(["hack/install-ate.sh", "delete", "ate-system"], check=False)  # main #2718: ate-setup subcommands
    # Old installs leave ate.dev CRDs behind; main dropped versions (e.g. sandboxconfigs v1alpha1 -> v1) that
    # Kubernetes won't remove from a CRD that stored them, so the next install fails. Delete them; install recreates.
    crds = [n for n in e.k("get", "crd", "-o", "name", check=False).stdout.split() if n.endswith(".ate.dev")]
    if crds:
        e.k("delete", *crds, "--wait=true", "--timeout=300s", check=False)
    clean_node_state(e)


def clean_node_state(e):
    """Benchmark hygiene: after teardown (no workers left), remove leftover node-local actor dirs and snapshot
    staging from the metal node so runs start clean (leftovers accumulated to 39k dirs / ~170 GB -> DiskPressure).
    Logs what was found first, as evidence for the leak."""
    node = e.k("get", "nodes", "-l", "bench-node=c3-metal", "-o", "jsonpath={.items[0].metadata.name}", check=False).stdout.strip()
    if not node:
        return
    script = ("cd /var/lib/ate 2>/dev/null || exit 0; "
              "echo leftover actors=$(ls actors 2>/dev/null | wc -l) snapshots=$(ls snapshots 2>/dev/null | wc -l) "
              "scratch=$(ls snapshot-scratch 2>/dev/null | wc -l); df -h /var | tail -1; "
              "for d in actors snapshots snapshot-scratch; do find $d -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null; done; "
              "echo after: actors=$(ls actors 2>/dev/null | wc -l); df -h /var | tail -1")
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "bench-node-clean", "namespace": "kube-system"},
           "spec": {"nodeName": node, "hostPID": True, "restartPolicy": "Never", "priorityClassName": "system-node-critical",
                    "tolerations": [{"operator": "Exists"}],
                    "containers": [{"name": "c", "image": "gcr.io/pmandewalkar-gke-dev/ate-images/busybox:1.36",
                                    "securityContext": {"privileged": True},
                                    "command": ["nsenter", "-t", "1", "-m", "--", "sh", "-c", script]}]}}
    e.k("delete", "pod", "-n", "kube-system", "bench-node-clean", "--ignore-not-found", "--wait=true", check=False)
    e.k("apply", "-f", "-", input=json.dumps(pod), check=False)
    t0 = time.time()
    while time.time() - t0 < 3600:  # rm of ~40k dirs can take many minutes; never delete the pod mid-run
        ph = e.k("get", "pod", "-n", "kube-system", "bench-node-clean", "-o", "jsonpath={.status.phase}", check=False).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            break
        time.sleep(15)
    out = e.k("logs", "-n", "kube-system", "bench-node-clean", check=False).stdout.strip()
    log(f"node-state cleanup on {node}: {out}")
    e.k("delete", "pod", "-n", "kube-system", "bench-node-clean", "--wait=false", check=False)


def set_upload_budget(e, budget):
    """Add --upload-memory-budget to the objectstore-plugin initContainer of the atelet DaemonSet."""
    ds = e.k("get", "ds", "-n", "ate-system", "-l", "app=atelet", "-o", "jsonpath={.items[0].metadata.name}").stdout.strip()
    names = e.k("get", "ds", "-n", "ate-system", ds, "-o",
                "jsonpath={range .spec.template.spec.initContainers[*]}{.name}{\"\\n\"}{end}").stdout.split()
    idx = names.index("objectstore-plugin")
    args = json.loads(e.k("get", "ds", "-n", "ate-system", ds, "-o",
                          f"jsonpath={{.spec.template.spec.initContainers[{idx}].args}}").stdout or "[]")
    args = [a for a in args if not a.startswith("--upload-memory-budget")] + [f"--upload-memory-budget={budget}"]
    patch = json.dumps([{"op": "replace", "path": f"/spec/template/spec/initContainers/{idx}/args", "value": args}])
    e.k("patch", "ds", "-n", "ate-system", ds, "--type=json", "-p", patch)
    e.run(["kubectl", "rollout", "status", "ds", "-n", "ate-system", ds, "--timeout=600s"], timeout=700)
    log(f"objectstore-plugin upload budget set to {budget} on {ds}")


def worker_resources(cpu_m, mem_ki):
    """WorkerPool template resources for OVERCOMMIT/OVERCOMMIT_MEM split over WORKERS (as deploy() always did)."""
    f = OVERCOMMIT / WORKERS
    f = min(f, 999000 / cpu_m)  # ateapi rejects worker cpu capacity >= 1000 cores
    fm = (OVERCOMMIT_MEM or OVERCOMMIT) / WORKERS
    return {"requests": {"cpu": "1", "memory": "8Gi"},
            "limits": {"cpu": f"{int(cpu_m * f)}m", "memory": f"{int(mem_ki * fm)}Ki"}}


def deploy(e, node):
    # main enforces ate-api authz: make kubectl-ate's SA and the runner bootstrap owners (as CI's orchestrator.py does).
    e.env["ATE_API_AUTHZ_BOOTSTRAP_OWNERS"] = ("system:serviceaccount:ate-system:ate-client,"
                                              "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner")
    # main (#2390, Oct 20) requires an ingress SPIFFE allowlist; the benchmark runner is the router client.
    e.env.setdefault("ATE_INGRESS_ALLOWED_SPIFFE_IDS", "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner")
    # main (#2544, Oct 19) requires a cluster-wide snapshot location (as hack/run-e2e-kind.sh sets it).
    e.env.setdefault("SNAPSHOT_LOCATION", f"gs://{e.c.get('bucket', BUCKET)}/")
    if WORKLOAD == "agentsession":
        e.env["BENCH_WORKLOAD_TEMPLATES"] = AGENT["templates"]
    e.run(["hack/install-ate.sh", "deploy", "ate-system", f"--otlp-endpoint={METER}"], timeout=3600)
    # Benchmark A/B: select a non-default gVisor SandboxConfig version (e.g. v7nightly) before workloads deploy.
    gv = os.environ.get("BENCH_GVISOR_VERSION")
    if gv and e.c["sandbox"] == "gvisor":
        e.k("patch", "sandboxconfig", "gvisor-default", "--type=merge", "-p", json.dumps({"spec": {"defaultVersion": gv}}))
        got = e.k("get", "sandboxconfig", "gvisor-default", "-o", "jsonpath={.spec.defaultVersion}").stdout.strip()
        log(f"gvisor SandboxConfig defaultVersion -> {got} (wanted {gv})")
        if got != gv:
            raise SystemExit(f"could not set gVisor version {gv} (got {got!r})")
    if UPLOAD_BUDGET:
        set_upload_budget(e, UPLOAD_BUDGET)
    if e.c["sandbox"] == "microvm":
        e.run(["hack/install-microvm-deps.sh", "--install"], timeout=3600)
    # Pin the worker to the target node: patch the WorkerPool CR's nodeSelector
    # (a supported API field) as soon as deploy.sh creates it. kubectl patch does
    # not touch last-applied, so later applies keep it.
    tmpl = {"nodeSelector": {"kubernetes.io/hostname": node}}
    if OVERCOMMIT > 0:
        alloc = json.loads(e.k("get", "node", node, "-o", "json").stdout)["status"]["allocatable"]
        c = alloc["cpu"]
        cpu_m = int(c[:-1]) if c.endswith("m") else int(float(c) * 1000)
        mem_ki = int(alloc["memory"][:-2])
        tmpl["resources"] = worker_resources(cpu_m, mem_ki)
        log(f"overcommit {OVERCOMMIT}x/node over {WORKERS} worker(s): worker limits {tmpl['resources']['limits']}")
    patch_json = json.dumps({"spec": {"template": tmpl}})
    pinner = subprocess.Popen(
        ["bash", "-c",
         f"for i in $(seq 1 600); do kubectl get workerpool {WORKER_POOL} -n benchmark-workloads >/dev/null 2>&1 && break; sleep 1; done; "
         f"kubectl patch workerpool {WORKER_POOL} -n benchmark-workloads --type=merge -p '{patch_json}'"],
        env=e.env, stdout=LOG, stderr=subprocess.STDOUT)
    e.run(deploy_sh("--deploy", "--worker-count", str(WORKERS),
                    "--sandbox-class", e.c["sandbox"], "--wait-timeout", "1500", *workload_deploy_args()), timeout=3600)
    pinner.wait(timeout=60)


def ensure_overcommit(e, node):
    """Skip-deploy path: re-patch WorkerPool worker limits if OVERCOMMIT changed; wait for the new pod."""
    if OVERCOMMIT <= 0:
        return
    alloc = json.loads(e.k("get", "node", node, "-o", "json").stdout)["status"]["allocatable"]
    c = alloc["cpu"]
    cpu_m = int(c[:-1]) if c.endswith("m") else int(float(c) * 1000)
    mem_ki = int(alloc["memory"][:-2])
    f = OVERCOMMIT / WORKERS
    f = min(f, 999000 / cpu_m)  # ateapi rejects worker cpu capacity >= 1000 cores
    want = f"{int(cpu_m * f)}m"
    wp = json.loads(e.k("get", "workerpool", WORKER_POOL, "-n", "benchmark-workloads", "-o", "json").stdout)
    cur = (wp["spec"].get("template", {}).get("resources") or {}).get("limits", {}).get("cpu")
    if cur == want:
        log(f"overcommit: worker cpu limit already {want}")
        return
    fm = (OVERCOMMIT_MEM or OVERCOMMIT) / WORKERS
    res = {"requests": {"cpu": "1", "memory": "8Gi"}, "limits": {"cpu": want, "memory": f"{int(mem_ki * fm)}Ki"}}
    e.k("patch", "workerpool", WORKER_POOL, "-n", "benchmark-workloads", "--type=merge",
        "-p", json.dumps({"spec": {"template": {"resources": res}}}))
    log(f"overcommit: re-patched worker limits {cur} -> {res['limits']} ({OVERCOMMIT}x/node); waiting for new pod")
    for _ in range(60):
        time.sleep(10)
        pods = json.loads(e.k("get", "pods", "-n", "benchmark-workloads", "-l",
                              f"ate.dev/worker-pool={WORKER_POOL}", "-o", "json").stdout)["items"]
        live = [p for p in pods if p["status"].get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")]
        if len(live) == WORKERS and all(p["spec"]["containers"][0].get("resources", {}).get("limits", {}).get("cpu") == want
                                        and all(cs.get("ready") for cs in p["status"].get("containerStatuses", [])) for p in live):
            time.sleep(60)  # let the worker register its new capacity
            return
    raise RuntimeError("overcommit: worker pod did not come back with new limits in 10 min")


def gate(e, node):
    """Abort unless the deployment is exactly what the test assumes."""
    pods = json.loads(e.k("get", "pods", "-n", "benchmark-workloads", "-l",
                          f"ate.dev/worker-pool={WORKER_POOL}", "-o", "json").stdout)["items"]
    live = [p for p in pods if p["status"].get("phase") == "Running" and not p["metadata"].get("deletionTimestamp")]
    nodes = {p["spec"].get("nodeName") for p in live}
    if len(live) != WORKERS or nodes != {node}:
        raise RuntimeError(f"gate: expected {WORKERS} worker(s) on {node}, got {[(p['metadata']['name'], p['spec'].get('nodeName')) for p in live]}")
    wp = json.loads(e.k("get", "workerpool", WORKER_POOL, "-n", "benchmark-workloads", "-o", "json").stdout)
    log(f"gate: worker {live[0]['metadata']['name']} on {node}; sandboxClasses={wp['spec'].get('sandboxClasses')}")
    if OVERCOMMIT > 0 and not all(c.get("resources", {}).get("limits", {}).get("cpu") for p in live for c in p["spec"]["containers"][:1]):
        raise RuntimeError("gate: overcommit requested but worker container has no cpu limit")
    log(f"gate: worker resources {[c.get('resources') for p in live for c in p['spec']['containers'][:1]]}")
    # atelet server metrics must reach Prometheus through the meter.
    e.prom_start()
    for _ in range(40):
        n = e.qv('count(ate_actor_stats_sampled_actors) or count(atelet_snapshot_size_bytes_count) '
                 'or count(ate_workerpool_workers)')
        if n:
            log(f"gate: server metrics present ({n} series)")
            break
        time.sleep(15)
    else:
        raise RuntimeError("gate: no atelet/ateapi metrics in Prometheus after 10 min (meter routing?)")
    counts = db_counts(e)
    log(f"gate: db counts {counts}")
    if counts.get("registered_sb", 0) != 0:
        raise RuntimeError(f"gate: benchmark actors already present: {counts}")
    if WORKLOAD == "agentsession":
        # boomer refuses to start sessions if glutton is missing or below the script's min_actor_memory.
        r = e.ate("get", "actor-template", "glutton", "-a", "benchmark-workloads", "-o", "json", check=False)
        out = (r.stdout or "") + (r.stderr or "")
        res = re.findall(r'"(cpuMillicores|memoryBytes)":\s*"?(\d+)', out)
        log(f"gate: glutton template {'resources ' + str(dict(res)) if res else 'NOT FOUND/unreadable: ' + out[-300:]}")


def db_counts(e):
    """Registered / Running from the control-plane DB (ground truth state)."""
    sql = ("SELECT (SELECT count(*) FROM substrate.actors), "
           f"(SELECT count(*) FROM substrate.actors WHERE name LIKE '{ACTOR_PREFIX}%'), "
           "(SELECT count(*) FROM substrate.worker_assignments);")
    r = e.k("exec", "-n", "ate-system", "postgres-0", "-c", "postgres", "--", "psql", "-U", "postgres",
            "-d", "atepg", "-At", "-F", ",", "-c", sql, check=False)
    try:
        a, sb, w = (int(x) for x in r.stdout.strip().split(","))
        return {"registered": a, "registered_sb": sb, "running_assigned": w}
    except Exception:
        return {"error": (r.stdout + (r.stderr or ""))[-300:]}


def reset_actors(e):
    """Delete any benchmark actors left after a runner exits (leaks from failed starts)."""
    for attempt in range(3):
        r = e.ate("get", "actor", "-a", "benchmark", "-o", "json", check=False)
        names = sorted(set(re.findall(r'"name":\s*"(' + re.escape(ACTOR_PREFIX) + r'[0-9a-f-]{36})"', r.stdout or "")))
        log(f"reset: {len(names)} leftover benchmark actors")
        if not names:
            break
        if len(names) > 4000:
            return False  # caller falls back to full reinstall
        cmd = (f"printf '%s\\n' {' '.join(names)} | xargs -P 16 -I{{}} "
               f"{e.clone}/bin/kubectl-ate delete actor {{}} -a benchmark >/dev/null 2>>{e.out}/reset_delete_errors.log")
        e.run(cmd, check=False, timeout=3600)
    time.sleep(30)
    c = db_counts(e)
    log(f"reset: db after cleanup {c}")
    return c.get("registered_sb", 1) == 0


# ---------------------------------------------------------------- runner job


def ladder(users, hold_fn, spawn):
    steps, prev, total, plan = [], 0, 0, []
    for u in users:
        ramp = math.ceil(max(0, u - prev) / spawn)
        hold = ramp + SETTLE_S + hold_fn(u)
        steps.append(f"{u}:{hold}s")
        plan.append(dict(users=u, start_offset=total, ramp_s=ramp, hold_s=hold))
        total += hold
        prev = u
    return ",".join(steps), total, plan


def probe_manifest(node, tag, secs, density=False):
    """ConfigMap + privileged hostPID Pod running node_probe.sh (or density_probe.sh) on `node`."""
    name = f"{'density' if density else 'node'}-probe-{tag}".lower().replace("_", "-")[:60].rstrip("-")
    script = (DENSITY_PROBE_SCRIPT if density else PROBE_SCRIPT).read_text()
    cmd = ["sh", "/probe/probe.sh", str(secs)] + ([str(DENSITY_PROBE_INTERVAL)] if density else [])
    cm = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": "benchmarking"},
          "data": {"probe.sh": script}}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": "benchmarking"},
           "spec": {"hostPID": True, "restartPolicy": "Never", "nodeName": node,
                    "tolerations": [{"operator": "Exists"}],
                    "containers": [{"name": "probe", "image": PROBE_IMAGE,
                                    "command": cmd,
                                    "securityContext": {"privileged": True},
                                    "volumeMounts": [{"name": "s", "mountPath": "/probe"}]}],
                    "volumes": [{"name": "s", "configMap": {"name": name}}]}}
    return name, yaml.safe_dump_all([cm, pod])


def launch_probe(e, node, tag, secs=None, density=False):
    """Run node_probe.sh (density_probe.sh if density) in a privileged hostPID pod on `node`; save its output. Non-blocking."""
    secs = secs or PROBE_SECS
    name, manifest = probe_manifest(node, tag, secs, density)
    out = e.out / f"probe_{tag}.txt"
    cmd = (f"kubectl delete pod -n benchmarking {name} --ignore-not-found --wait=true >/dev/null 2>&1; "
           f"kubectl apply -f - >/dev/null && "
           f"kubectl wait -n benchmarking pod/{name} --for=jsonpath='{{.status.phase}}'=Succeeded --timeout={secs + 240}s >/dev/null; "
           f"kubectl logs -n benchmarking {name} > {out} 2>&1; "
           f"kubectl delete pod,configmap -n benchmarking {name} --ignore-not-found --wait=false >/dev/null 2>&1")
    log(f"probe {tag}: launching {'density_probe' if density else 'node_probe'} on {node} for {secs}s -> {out.name}")
    return _spawn(cmd, e.env, manifest)


def whole_step_probes(plan, label, t_sub, users_filter=None):
    """density_probe.sh over each step (start + 60 s .. end - 30 s): [(abs_time, tag, secs, density=True)].
    t_sub + 45 s approximates when the runner's ladder starts (as the existing probe schedules do)."""
    out = []
    for p_ in plan:
        if users_filter and p_["users"] not in users_filter:
            continue
        tag = f"{label}_{p_['users']}u" + ("_hold" if p_.get("phase") == "hold_ext" else "")
        out.append((t_sub + 45 + p_["start_offset"] + 60, tag, max(60, p_["hold_s"] - 90), True))
    return out


def _spawn(cmd, env, stdin_text):
    pr = subprocess.Popen(["bash", "-c", cmd], env=env, stdin=subprocess.PIPE, text=True,
                          stdout=LOG, stderr=subprocess.STDOUT)
    pr.stdin.write(stdin_text)
    pr.stdin.close()
    return pr


def job_name(e, test, run_id):
    name = f"{e.name}_{test}".replace("_", "-")
    return f"{'as' if WORKLOAD == 'agentsession' else 'sw'}-{name}-{run_id[-6:]}".lower()[:63]


def runner_load_args(ladder_spec, spawn, min_wait, max_wait):
    """Locust/boomer flags appended to the runner Job args for the selected workload."""
    if WORKLOAD == "agentsession":
        # Every knob explicit: in headless runs boomer gets the master's parsed options (defaults included),
        # and the AgentSessionUser default lifecycle hibernates after every step.
        a = ["--ladder", ladder_spec, "--ladder-spawn-rate", str(spawn),
             "--lifecycle-mode", LIFECYCLE_MODE or "none",
             "--agentsession-script", AGENT["script"],
             "--agentsession-think-scale", str(AGENT["think_scale"]),
             "--agentsession-phase-in", str(AGENT["phase_in"])]
        if AGENT["resume_retry_budget"] is not None:
            a += ["--agentsession-resume-retry-budget", str(AGENT["resume_retry_budget"])]
        return a
    a = ["--ladder", ladder_spec, "--ladder-spawn-rate", str(spawn),
         "--min-wait-time", str(min_wait), "--max-wait-time", str(max_wait)]
    if SWEPERF_TEMPLATE:
        a += ["--sweperf-template", SWEPERF_TEMPLATE]
    if LIFECYCLE_MODE:
        a += ["--lifecycle-mode", LIFECYCLE_MODE]
    return a


def render_job(e, test, ladder_spec, total_s, first_users, spawn, min_wait, max_wait, run_id):
    """Runner Job manifest (multi-doc YAML) from the clone's runner-job.yaml.tmpl."""
    job = job_name(e, test, run_id)
    test_file = ("/app/tests/agentsession.py" if WORKLOAD == "agentsession" else "/app/tests/sweperf.py") \
        + ",/app/shapes/ladder_shape.py"
    tmpl = (e.clone / "benchmarking/automation/manifests/runner-job.yaml.tmpl").read_text()
    subs = dict(JOB_NAME=job, IMAGE=LOCUST_IMAGE, TEST_FILE=test_file,
                DURATION=f"{total_s}s", USERS=str(first_users), TAG=COMMIT, NAME=f"{e.name}_{test}",
                DEST=f"gs://{e.c.get('bucket', BUCKET)}/sweperf-stress/{run_id}", RUNNER_CPU="6", RUNNER_MEMORY="12Gi")
    for k_, v in subs.items():
        tmpl = tmpl.replace("${" + k_ + "}", v)
    docs = [d for d in yaml.safe_load_all(tmpl) if d]
    for d in docs:
        if d.get("kind") == "Job":
            pod = d["spec"]["template"]
            pod["metadata"].setdefault("annotations", {}).update(
                {"prometheus.io/scrape": "true", "prometheus.io/port": "8001"})
            pod["metadata"]["labels"]["bench-case"] = e.name
            pod["metadata"]["labels"]["bench-test"] = test
            pod["spec"]["nodeSelector"] = {"bench-node": "loadgen"}
            if RUNNER_ENV:
                pod["spec"]["containers"][0].setdefault("env", []).extend(
                    {"name": k_, "value": v_} for k_, v_ in RUNNER_ENV.items())
            pod["spec"]["containers"][0]["args"] += runner_load_args(ladder_spec, spawn, min_wait, max_wait)
            d["spec"]["activeDeadlineSeconds"] = total_s + 3600
    return job, yaml.safe_dump_all(docs)


def run_job(e, test, ladder_spec, total_s, first_users, spawn, min_wait, max_wait, run_id, probes=None, node=None,
            monitor=None):
    """Submit the runner Job and follow it. probes: [(abs_time, tag)] node_probe or [(abs_time, tag, secs, density)].
    monitor(job, t_submit, samples) -> reason str to stop the ladder early (job deleted after its logs are saved)."""
    job, manifest = render_job(e, test, ladder_spec, total_s, first_users, spawn, min_wait, max_wait, run_id)
    (e.out / f"{test}_job.yaml").write_text(manifest)
    e.k("delete", "job", job, "-n", "benchmarking", "--ignore-not-found", check=False)
    e.k("apply", "-f", "-", input=manifest)
    log(f"{test}: job {job} submitted; ladder {ladder_spec}; total {total_s}s")
    t_submit = time.time()
    # Wait for the pod to start, record the ladder t0 from locust_users appearing.
    deadline = t_submit + total_s + 3600
    status = "unknown"
    stop_reason = None
    sampler_next = 0
    samples = []
    probes = list(probes or [])   # [(abs_time, tag)] or [(abs_time, tag, secs, density)]
    probe_procs = []
    while time.time() < deadline:
        j = json.loads(e.k("get", "job", job, "-n", "benchmarking", "-o", "json").stdout)
        st = j.get("status", {})
        if st.get("succeeded"):
            status = "complete"
            break
        if st.get("failed"):
            status = "failed"
            break
        if time.time() >= sampler_next:
            c = db_counts(e)
            c["t"] = int(time.time())
            samples.append(c)
            sampler_next = time.time() + 60
            if len(samples) >= 6 and all((x.get("registered_sb") or 0) == 0 for x in samples):
                e.k("delete", "job", job, "-n", "benchmarking", "--wait=false", check=False)
                raise RuntimeError("no actors registered after ~5 min (authz/workload broken?) - aborting job")
        if monitor:
            try:
                stop_reason = monitor(job, t_submit, samples)
            except Exception as ex:  # a broken check must not kill the run
                log(f"{test}: monitor error (ignored): {ex}")
            if stop_reason:
                status = "stopped"
                log(f"{test}: stopping ladder: {stop_reason}")
                break
        for pt in list(probes):
            if time.time() >= pt[0]:
                probe_procs.append(launch_probe(e, node, pt[1], *pt[2:]) if len(pt) > 2 else launch_probe(e, node, pt[1]))
                probes.remove(pt)
        time.sleep(20)
    if status == "stopped":
        # Save the logs first (they go with the pod), then stop the ramp.
        logs = e.k("logs", f"job/{job}", "-n", "benchmarking", "--tail=-1", check=False).stdout or ""
        e.k("delete", "job", job, "-n", "benchmarking", "--wait=false", check=False)
    for pr in probe_procs:
        try:
            pr.wait(timeout=600)
        except Exception:
            pass
    (e.out / f"{test}_db_samples.json").write_text(json.dumps(samples, indent=1))
    if status != "stopped":
        logs = e.k("logs", f"job/{job}", "-n", "benchmarking", "--tail=-1", check=False).stdout or ""
    (e.out / f"{test}_runner_logs.txt").write_text(logs)
    m = re.search(r"(gs://\S+/runs/\S+?)/status\.json", logs)
    gcs_prefix = m.group(1) if m else None
    log(f"{test}: job {status}; results {gcs_prefix}")
    return dict(job=job, status=status, submitted=int(t_submit), finished=int(time.time()),
                gcs_prefix=gcs_prefix, samples=samples, stop_reason=stop_reason)


# ---------------------------------------------------------------- analysis


def step_windows(e, jobinfo, plan):
    """Find ladder t0 from the locust_users series of this job's pod, then
    derive each step's measurement window from the plan offsets."""
    series = e.qrange(f'sum(locust_users{{job="boomer-worker",pod=~"{jobinfo["job"]}.*"}})',
                      jobinfo["submitted"], jobinfo["finished"], 5)
    pts = [(int(float(t)), float(v)) for s in series for t, v in s["values"]]
    t_first = min((t for t, v in pts if v > 0), default=jobinfo["submitted"] + 60)
    t0 = t_first - 5
    wins = []
    for p in plan:
        s = t0 + p["start_offset"]
        wins.append(dict(users=p["users"], step_start=s, start=s + p["ramp_s"] + p.get("settle_s", SETTLE_S),
                         end=s + p["hold_s"], phase=p.get("phase", "ramp")))
    return wins, pts


def analyze_step(e, w, pod_re, node):
    a, b = w["start"], w["end"]
    d = max(1, b - a)
    sel = f'job="boomer-worker",pod=~"{pod_re}"'  # dedupe: pods are also scraped by kubernetes-pods (2x)

    def inc(name, status):
        return e.qv(f'sum(increase(locust_requests_total{{{sel},name="{name}",status="{status}"}}[{d}s]))', b) or 0.0

    def quant(name, qt):
        v = e.qv(f'histogram_quantile({qt}, sum by (le) (increase(locust_request_duration_milliseconds_bucket'
                 f'{{{sel},name="{name}",status="success"}}[{d}s])))', b)
        return None if v is None else round(v / 1000.0, 3)

    def avg_over(expr):
        return e.qv(f'avg_over_time(({expr})[{d}s:15s])', b)

    def max_over(expr):
        return e.qv(f'max_over_time(({expr})[{d}s:15s])', b)

    r = {"users_target": w["users"], "window": [a, b]}
    r["users_live_avg"] = avg_over(f'sum(locust_users{{{sel}}})')
    ops = ["CreateActor", "ResumeActor", "SuspendActor", "SuspendActorAsync", "CycleCEL", "ResumeToFirstExec", "TaskWallClock", "DeleteActor"]
    tot_ok = tot_fail = 0.0
    for op in ops:
        ok, fail = inc(op, "success"), inc(op, "failure")
        r[f"{op}_ok_per_s"] = round(ok / d, 3)
        r[f"{op}_fail"] = int(fail)
        if op in ("CreateActor", "ResumeActor", "SuspendActor", "SuspendActorAsync", "CycleCEL"):
            tot_ok += ok
            tot_fail += fail
        for qt in (0.5, 0.95, 0.99):
            r[f"{op}_p{int(qt*100)}_s"] = quant(op, qt)
    r["fail_rate"] = round(tot_fail / max(1.0, tot_ok + tot_fail), 4)
    r["resumes_per_s"] = r["ResumeActor_ok_per_s"]
    r["cycles_per_s"] = r["CycleCEL_ok_per_s"]
    cyc_mean = e.qv(f'sum(increase(locust_request_duration_milliseconds_sum{{{sel},name="CycleCEL",status="success"}}[{d}s]))'
                    f' / sum(increase(locust_request_duration_milliseconds_count{{{sel},name="CycleCEL",status="success"}}[{d}s]))', b)
    r["cycle_cel_mean_s"] = None if cyc_mean is None else round(cyc_mean / 1000, 3)
    # Active (executing) by Little's law: completed cycles/s x mean in-sandbox time.
    r["active_littles_law"] = None if cyc_mean is None else round(r["cycles_per_s"] * cyc_mean / 1000, 1)
    # Running: atelet-sampled actors on the worker (server side).
    r["running_atelet_avg"] = avg_over('sum(ate_actor_stats_sampled_actors)')
    r["running_atelet_max"] = max_over('sum(ate_actor_stats_sampled_actors)')
    # Node resources (cAdvisor root cgroup relabelled container="node").
    nsel = f'container="node",kubernetes_io_hostname="{node}"'
    r["node_cpu_cores_avg"] = avg_over(f'sum(rate(container_cpu_usage_seconds_total{{{nsel}}}[1m]))')
    r["node_mem_gib_max"] = (max_over(f'sum(container_memory_working_set_bytes{{{nsel}}})') or 0) / 2**30
    r["actor_mem_gib_max"] = (max_over('sum(ate_actor_stats_memory_working_set_bytes)') or 0) / 2**30
    for res in ("cpu", "memory", "io"):
        r[f"psi_{res}_some_pct"] = avg_over(f'100*sum(rate(container_pressure_{res}_waiting_seconds_total{{{nsel}}}[1m]))')
        r[f"psi_{res}_full_pct"] = avg_over(f'100*sum(rate(container_pressure_{res}_stalled_seconds_total{{{nsel}}}[1m]))')
    r["node_disk_read_mbs"] = (avg_over(f'sum(rate(container_fs_reads_bytes_total{{{nsel}}}[1m]))') or 0) / 1e6
    r["node_disk_write_mbs"] = (avg_over(f'sum(rate(container_fs_writes_bytes_total{{{nsel}}}[1m]))') or 0) / 1e6
    r["node_net_rx_mbs"] = (avg_over(f'sum(rate(container_network_receive_bytes_total{{{nsel}}}[1m]))') or 0) / 1e6
    r["node_net_tx_mbs"] = (avg_over(f'sum(rate(container_network_transmit_bytes_total{{{nsel}}}[1m]))') or 0) / 1e6
    # Server-side snapshot timings (atelet via meter).
    for kind in ("restore", "checkpoint"):
        for qt in (0.5, 0.95):
            v = e.qv(f'histogram_quantile({qt}, sum by (le) (increase(ate_actor_{kind}_duration_seconds_bucket'
                     f'{{ate_snapshot_phase="total"}}[{d}s])))', b)
            r[f"server_{kind}_p{int(qt*100)}_s"] = None if v is None else round(v, 3)
    r["snapshot_mb_avg"] = None
    sz = e.qv(f'sum(increase(atelet_snapshot_size_bytes_sum[{d}s])) / sum(increase(atelet_snapshot_size_bytes_count[{d}s]))', b)
    if sz:
        r["snapshot_mb_avg"] = round(sz / 1e6, 1)
    # Worker pod (ateom + sandboxes) CPU and CFS throttling; atelet CPU.
    wsel = 'namespace="benchmark-workloads",pod=~"benchmark-worker.*"'
    r["worker_cpu_cores"] = avg_over(f'sum(rate(container_cpu_usage_seconds_total{{{wsel},container!=""}}[1m]))')
    r["worker_throttle_pct"] = avg_over(
        f'100*sum(rate(container_cpu_cfs_throttled_periods_total{{{wsel}}}[1m]))/sum(rate(container_cpu_cfs_periods_total{{{wsel}}}[1m]))')
    r["atelet_cpu_cores"] = avg_over('sum(rate(container_cpu_usage_seconds_total{namespace="ate-system",pod=~"atelet.*",container!=""}[1m]))')
    # Checkpoint/restore p50 by phase: which phase grows first.
    for kind in ("restore", "checkpoint"):
        res = e.q(f'histogram_quantile(0.5, sum by (le, ate_snapshot_phase) (increase(ate_actor_{kind}_duration_seconds_bucket[{d}s])))', b)
        r[f"{kind}_phase_p50_s"] = {x["metric"].get("ate_snapshot_phase", "?"): round(float(x["value"][1]), 3)
                                    for x in res if x["value"][1] not in ("NaN", "+Inf")}
    # Lifecycle v2: background upload / detach (atelet), and actors whose upload is overdue.
    for nm, key in (("atelet_snapshot_upload_duration_seconds", "async_upload"),
                    ("atelet_snapshot_detach_duration_seconds", "async_detach")):
        for qt in (0.5, 0.95):
            v = e.qv(f'histogram_quantile({qt}, sum by (le) (increase({nm}_bucket[{d}s])))', b)
            r[f"{key}_p{int(qt*100)}_s"] = None if v is None else round(v, 3)
        c = e.qv(f'sum(increase({nm}_count[{d}s]))', b)
        r[f"{key}_per_s"] = None if c is None else round(c / d, 3)
    r["upload_overdue_actors_max"] = max_over('sum(atelet_snapshot_upload_overdue_actors)')
    # GCE boot disk counters (Cloud Monitoring), per second over the window.
    r.update(gce_disk(e, a, b))
    # Validity guards.
    r["loadgen_cpu_cores"] = avg_over(f'sum(rate(container_cpu_usage_seconds_total{{{sel},container="runner"}}[1m]))')
    r["loadgen_throttle_pct"] = avg_over(
        f'100*sum(rate(container_cpu_cfs_throttled_periods_total{{{sel}}}[1m]))/sum(rate(container_cpu_cfs_periods_total{{{sel}}}[1m]))')
    r["router_cpu_cores"] = avg_over('sum(rate(container_cpu_usage_seconds_total{namespace="ate-system",pod=~"atenet-router.*",container!=""}[1m]))')
    r["apiserver_cpu_cores"] = avg_over('sum(rate(container_cpu_usage_seconds_total{namespace="ate-system",pod=~"ate-api-server.*",container!=""}[1m]))')
    r["postgres_cpu_cores"] = avg_over('sum(rate(container_cpu_usage_seconds_total{namespace="ate-system",pod=~"postgres.*",container!=""}[1m]))')
    for k_, v in list(r.items()):
        if isinstance(v, float):
            r[k_] = round(v, 3)
    return r


def gce_disk(e, a, b):
    """Boot-disk IOPS / MB/s (incl. throttled) from Cloud Monitoring for [a, b]."""
    out = {}
    if not getattr(e, "instance_id", None):
        return out
    import datetime as _dt
    tsf = lambda t: _dt.datetime.fromtimestamp(t, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    token = subprocess.run(["gcloud", "auth", "print-access-token"], capture_output=True, text=True).stdout.strip()
    for m in ("write_bytes_count", "write_ops_count", "read_bytes_count", "read_ops_count",
              "throttled_write_bytes_count", "throttled_write_ops_count"):
        params = {"filter": f'metric.type="compute.googleapis.com/instance/disk/{m}" AND resource.labels.instance_id="{e.instance_id}"',
                  "interval.startTime": tsf(a), "interval.endTime": tsf(b),
                  "aggregation.alignmentPeriod": f"{max(60, b - a)}s", "aggregation.perSeriesAligner": "ALIGN_SUM",
                  "aggregation.crossSeriesReducer": "REDUCE_SUM"}
        url = "https://monitoring.googleapis.com/v3/projects/pmandewalkar-gke-dev/timeSeries?" + urllib.parse.urlencode(params)
        try:
            d = json.load(urllib.request.urlopen(urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"}), timeout=60))
            tot = sum(float(p["value"].get("int64Value", p["value"].get("doubleValue", 0)))
                      for ts_ in d.get("timeSeries", []) for p in ts_["points"])
            scale = 1e6 if "bytes" in m else 1.0
            out["gce_" + m.replace("_count", "").replace("bytes", "mbs").replace("ops", "iops")] = round(tot / (b - a) / scale, 1)
        except Exception:
            pass
    return out


def verdict(r, sla_s):
    live_ok = ((r.get("users_live_avg") or 0) >= 0.98 * r["users_target"]
               or (r.get("db_registered_max") or 0) >= 0.98 * r["users_target"])
    functional = r["fail_rate"] < 0.05 and live_ok and (r.get("ResumeActor_p95_s") or 1e9) < 10
    sla = functional and (r.get("ResumeActor_p95_s") or 1e9) <= sla_s
    return dict(functional_pass=functional, sla_pass=sla, users_reached=live_ok)


def density_verdict(rows):
    """Density pass: (almost) every actor placed, few failures, and single wakes still healthy.
    No p95 latency bar: wake health is the step's ResumeActor p50 against the first (lightest) step's."""
    base = next((r.get("ResumeActor_p50_s") for r in rows if r.get("ResumeActor_p50_s")), None)
    for r in rows:
        placed = (r.get("db_registered_max") or 0) >= 0.98 * r["users_target"]
        p50 = r.get("ResumeActor_p50_s")
        wake_ok = p50 is not None and base is not None and p50 <= max(2.0, 2.0 * base)
        r.update(density_placed=placed, density_wake_ok=wake_ok,
                 density_pass=placed and wake_ok and r["fail_rate"] < 0.05)
        log(f"density users={r['users_target']}: registered={r.get('db_registered_max')} assigned={r.get('db_running_assigned_max')} "
            f"fail={r['fail_rate']} wakes/s={r['resumes_per_s']} resumeP50={p50} (base {base}) "
            f"node_mem={round(r.get('node_mem_gib_max') or 0,1)}GiB actor_mem={round(r.get('actor_mem_gib_max') or 0,1)}GiB pass={r['density_pass']}")
    return rows


def analyze_test(e, test, jobinfo, plan, node, db_samples):
    wins, pts = step_windows(e, jobinfo, plan)
    sla_s = 0.15 if e.c["sandbox"] == "microvm" else 0.30
    rows = []
    for w in wins:
        if w["end"] > jobinfo["finished"]:
            w["end"] = jobinfo["finished"]
        if w["end"] - w["start"] < 60:
            continue
        r = analyze_step(e, w, jobinfo["job"] + ".*", node)
        near = [s for s in db_samples if w["start"] - 60 <= s.get("t", 0) <= w["end"] + 60 and "registered" in s]
        if near:
            r["db_registered_max"] = max(s["registered_sb"] for s in near)
            r["db_running_assigned_max"] = max(s["running_assigned"] for s in near)
        r.update(verdict(r, sla_s))
        rows.append(r)
        log(f"{test} users={r['users_target']}: live={r['users_live_avg']} resumes/s={r['resumes_per_s']} "
            f"fail={r['fail_rate']} resumeP95={r['ResumeActor_p95_s']} active~{r['active_littles_law']} "
            f"running~{r['running_atelet_avg']} cpu={r['node_cpu_cores_avg']} psi_io={r['psi_io_some_pct']} "
            f"pass={r['functional_pass']}")
    # Raw time series export for the GCS deep-dive.
    raw = {}
    sel = f'job="boomer-worker",pod=~"{jobinfo["job"]}.*"'  # dedupe (2x)
    for key, expr in {
        "users": f'sum(locust_users{{{sel}}})',
        "ok_rate_by_op": f'sum by (name) (rate(locust_requests_total{{{sel},status="success"}}[1m]))',
        "fail_rate_by_op": f'sum by (name) (rate(locust_requests_total{{{sel},status="failure"}}[1m]))',
        "running_atelet": 'sum(ate_actor_stats_sampled_actors)',
        "node_cpu": f'sum(rate(container_cpu_usage_seconds_total{{container="node",kubernetes_io_hostname="{node}"}}[1m]))',
        "node_mem": f'sum(container_memory_working_set_bytes{{container="node",kubernetes_io_hostname="{node}"}})',
        "psi_io": f'100*sum(rate(container_pressure_io_waiting_seconds_total{{container="node",kubernetes_io_hostname="{node}"}}[1m]))',
        "psi_cpu": f'100*sum(rate(container_pressure_cpu_waiting_seconds_total{{container="node",kubernetes_io_hostname="{node}"}}[1m]))',
        "psi_mem": f'100*sum(rate(container_pressure_memory_waiting_seconds_total{{container="node",kubernetes_io_hostname="{node}"}}[1m]))',
    }.items():
        raw[key] = e.qrange(expr, jobinfo["submitted"], jobinfo["finished"], 15)
    (e.out / f"{test}_prom_raw.json").write_text(json.dumps(raw))
    (e.out / f"{test}_steps.json").write_text(json.dumps(rows, indent=1))
    return rows


def best(rows, key, need="functional_pass"):
    ok = [r for r in rows if r.get(need)]
    return max(ok, key=lambda r: r.get(key) or 0) if ok else None


# ---------------------------------------------------------------- agentsession (idle-active density)


def agent_ladder(users, hold_s, spawn, settle_s, final_hold_s=0):
    """Upward-only ladder: each step ramps to N users (= N resident actors with --lifecycle-mode none) at
    `spawn`/s, settles `settle_s` (phase-in + first wakes), then holds `hold_s`. final_hold_s > 0 appends a
    same-N 'hold_ext' step (no spawn message; kept only if the top step is clean, see make_agent_monitor)."""
    steps, plan, prev, total = [], [], 0, 0
    for u in users:
        if u <= prev:
            raise ValueError(f"agentsession ladder must strictly increase (lowering users leaks actors): {users}")
        ramp = math.ceil((u - prev) / spawn)
        h = ramp + settle_s + hold_s
        steps.append(f"{u}:{h}s")
        plan.append(dict(users=u, start_offset=total, ramp_s=ramp, settle_s=settle_s, hold_s=h, phase="ramp"))
        total += h
        prev = u
    if final_hold_s > 0:
        steps.append(f"{prev}:{final_hold_s}s")
        plan.append(dict(users=prev, start_offset=total, ramp_s=0, settle_s=0, hold_s=final_hold_s, phase="hold_ext"))
        total += final_hold_s
    return ",".join(steps), total, plan


# Error classes for boomer WARN/ERROR lines (msg + err), first match wins.
BOOMER_ERR_CLASSES = [
    ("no_worker_has_room", r"no worker has room"),
    ("worker_is_full", r"worker is full"),
    ("never_served", r"never served"),
    ("resource_exhausted", r"ResourceExhausted"),
    ("deadline_exceeded", r"DeadlineExceeded|context deadline exceeded|Client\.Timeout|timeout"),
    ("unavailable", r"Unavailable|\b503\b"),
    ("gateway_timeout", r"\b504\b"),
    ("too_many_requests", r"\b429\b"),
    ("crashed", r"CRASHED|[Cc]rash"),
    ("conn_refused", r"connection refused"),
    ("conn_reset", r"connection reset|broken pipe|\bEOF\b"),
    ("no_route", r"no route to host|network is unreachable"),
    ("not_found", r"NotFound|\b404\b"),
    ("already_exists", r"AlreadyExists"),
    ("template_refused", r"min_actor_memory|GetActorTemplate"),
    ("oom", r"OOM|out of memory"),
]
# boomer agentsession log messages that are failures with no locust failure row of their own.
EXTRA_FAIL_MSGS = {"agent session wake failed": "wake_failed",
                   "agentsession start failed; goroutine will retry next iter": "start_failed"}
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TS = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:\d\d)?")


def _ts(s):
    m = _TS.match(s or "")
    if not m:
        return None
    t = dt.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")
    tz = m.group(3) or "Z"
    off = 0 if tz == "Z" else (1 if tz[0] == "+" else -1) * (int(tz[1:3]) * 3600 + int(tz[4:6]) * 60)
    return t.replace(tzinfo=dt.timezone.utc).timestamp() - off + float(m.group(2) or 0)


def err_class(text):
    for cls, pat in BOOMER_ERR_CLASSES:
        if re.search(pat, text):
            return cls
    return "other"


def norm_err(text):
    t = _UUID.sub("<uid>", text)
    t = re.sub(r"\b[0-9a-f]{16,}\b", "<hex>", t)
    t = re.sub(r"\d+(\.\d+)?(ms|s|µs|us)?", "N", t)
    return t[:200]


def parse_runner_events(logs):
    """Runner logs -> (events, first_t). events: boomer WARN/ERROR lines as dicts
    {t, level, msg, cls, err, extra}; extra names a failure that has no locust failure row
    (start failures not caused by CreateActor/CreateAtespace, and wake failures). Python locust
    ERROR/Traceback lines are returned with msg 'locust_error'."""
    events, first_t = [], None
    for line in logs.splitlines():
        if line.startswith("[boomer] {"):
            try:
                o = json.loads(line[9:])
            except ValueError:
                continue
            t = _ts(o.get("time"))
            if t and (first_t is None or t < first_t):
                first_t = t
            lvl = o.get("level", "")
            if lvl not in ("WARN", "ERROR"):
                continue
            msg, err = str(o.get("msg", "")), str(o.get("err", ""))
            extra = EXTRA_FAIL_MSGS.get(msg)
            if extra == "start_failed" and re.match(r"(createActor|ensureAtespace):", err):
                extra = None  # already a CreateActor/CreateAtespace failure row
            events.append(dict(t=t, level=lvl, msg=msg, cls=err_class(msg + " " + err), err=norm_err(err), extra=extra))
        elif line.startswith("[locust]") and re.search(r"Traceback|\bERROR\b|CRITICAL", line):
            events.append(dict(t=None, level="ERROR", msg="locust_error", cls=err_class(line), err=norm_err(line[9:]), extra=None))
    return events, first_t


def summarize_events(events, a, b):
    """Counts of boomer WARN/ERROR events with a <= t < b (untimed locust errors are counted in every window)."""
    from collections import Counter
    sel = [x for x in events if x["t"] is None or a <= x["t"] < b]
    by_msg, by_cls, by_msg_cls, by_err, extra = Counter(), Counter(), Counter(), Counter(), Counter()
    for x in sel:
        by_msg[x["msg"]] += 1
        by_cls[x["cls"]] += 1
        by_msg_cls[f"{x['msg']} | {x['cls']}"] += 1
        by_err[x["err"]] += 1
        if x["extra"]:
            extra[x["extra"]] += 1
    return dict(log_events=len(sel), log_by_msg=dict(by_msg.most_common(25)), log_by_class=dict(by_cls.most_common()),
                log_by_msg_class=dict(by_msg_cls.most_common(30)), log_top_errors=dict(by_err.most_common(10)),
                log_extra_failures=dict(extra), log_extra_failures_total=sum(extra.values()))


def agent_requests(e, sel, d, b):
    """{name: {"success": n, "failure": n}} over the window, every request name the runner recorded."""
    out = {}
    for x in e.q(f'sum by (name, status) (increase(locust_requests_total{{{sel}}}[{d}s]))', b):
        nm, stt = x["metric"].get("name", "?"), x["metric"].get("status", "?")
        try:
            v = float(x["value"][1])
        except (KeyError, ValueError, IndexError):
            continue
        if not math.isnan(v):
            out.setdefault(nm, {"success": 0, "failure": 0})[stt] = round(v)
    return out


def agent_fail_rate(reqs, ev):
    """Failures / attempts over all request names except TimeToWake (a summary row duplicating the wake),
    plus the log-only failures (wake pings of resident actors, session starts that never served)."""
    ok = sum(v.get("success", 0) for n, v in reqs.items() if n != "TimeToWake")
    fail = sum(v.get("failure", 0) for n, v in reqs.items() if n != "TimeToWake")
    extra = ev.get("log_extra_failures_total", 0)
    return round((fail + extra) / max(1.0, ok + fail + extra), 4), ok, fail, extra


def analyze_step_agent(e, w, pod_re, node, events, log_first_t=None):
    a, b = w["start"], w["end"]
    d = max(1, b - a)
    sel = f'job="boomer-worker",pod=~"{pod_re}"'  # dedupe: pods are also scraped by kubernetes-pods (2x)

    def avg_over(expr):
        return e.qv(f'avg_over_time(({expr})[{d}s:15s])', b)

    def max_over(expr):
        return e.qv(f'max_over_time(({expr})[{d}s:15s])', b)

    def min_over(expr):
        return e.qv(f'min_over_time(({expr})[{d}s:15s])', b)

    r = {"users_target": w["users"], "phase": w.get("phase", "ramp"), "window": [a, b]}
    r["users_live_avg"] = avg_over(f'sum(locust_users{{{sel}}})')
    # Requests and failures by name/status (all names, not a fixed list).
    reqs = agent_requests(e, sel, d, b)
    r["requests"] = reqs
    r["failures_by_name"] = {n: v["failure"] for n, v in reqs.items() if v.get("failure")}
    ev = summarize_events(events, a, b)
    r.update(ev)
    if log_first_t and a < log_first_t:
        r["logs_truncated"] = True  # kubectl logs only holds the newest rotated container log
    r["fail_rate"], r["req_ok"], r["req_fail"], r["log_only_fail"] = agent_fail_rate(reqs, ev)
    steps_ok = sum(v.get("success", 0) for n, v in reqs.items() if n.startswith("Step_"))
    r["steps_ok_per_s"] = round(steps_ok / d, 3)
    r["creates_ok"] = reqs.get("CreateActor", {}).get("success", 0)
    # Latency percentiles (s) of every op, plus all Step_* together. Buckets end at 32.768 s.
    lat = {}
    for qt in (0.5, 0.95, 0.99):
        for x in e.q(f'histogram_quantile({qt}, sum by (le, name) (increase(locust_request_duration_milliseconds_bucket'
                     f'{{{sel},status="success"}}[{d}s])))', b):
            try:
                v = float(x["value"][1])
            except (KeyError, ValueError, IndexError):
                continue
            if not math.isnan(v) and not math.isinf(v):
                lat.setdefault(x["metric"].get("name", "?"), {})[f"p{int(qt*100)}"] = round(v / 1000, 3)
        v = e.qv(f'histogram_quantile({qt}, sum by (le) (increase(locust_request_duration_milliseconds_bucket'
                 f'{{{sel},status="success",name=~"Step_.*"}}[{d}s])))', b)
        if v is not None:
            lat.setdefault("Step_ALL", {})[f"p{int(qt*100)}"] = round(v / 1000, 3)
    r["latency_s"] = lat
    r["latency_hist_cap_s"] = 32.768
    for op in ("CreateActor", "ResumeActor", "WakeFirstTouch", "TimeToWake", "DeleteActor", "Step_ALL"):
        for p in ("p50", "p95", "p99"):
            r[f"{op}_{p}_s"] = lat.get(op, {}).get(p)
    # BurnCPU iteration rate: drops under CPU contention while Step_* latency stays flat.
    for qt in (0.1, 0.5):
        r[f"burn_iters_per_s_p{int(qt*100)}"] = e.qv(
            f'histogram_quantile({qt}, sum by (le) (increase(agentsession_burn_cpu_iterations_per_second_bucket{{{sel}}}[{d}s])))', b)
    # Running actors (server side).
    r["running_atelet_avg"] = avg_over('sum(ate_actor_stats_sampled_actors)')
    r["running_atelet_max"] = max_over('sum(ate_actor_stats_sampled_actors)')
    r["running_atelet_min"] = min_over('sum(ate_actor_stats_sampled_actors)')
    # Node resources (cAdvisor root cgroup relabelled container="node").
    nsel = f'container="node",kubernetes_io_hostname="{node}"'
    r["node_cpu_cores_avg"] = avg_over(f'sum(rate(container_cpu_usage_seconds_total{{{nsel}}}[1m]))')
    r["node_cpu_cores_max"] = max_over(f'sum(rate(container_cpu_usage_seconds_total{{{nsel}}}[1m]))')
    r["node_mem_gib_max"] = (max_over(f'sum(container_memory_working_set_bytes{{{nsel}}})') or 0) / 2**30
    r["node_mem_gib_avg"] = (avg_over(f'sum(container_memory_working_set_bytes{{{nsel}}})') or 0) / 2**30
    r["actor_mem_gib_max"] = (max_over('sum(ate_actor_stats_memory_working_set_bytes)') or 0) / 2**30
    for res in ("cpu", "memory", "io"):
        r[f"psi_{res}_some_pct"] = avg_over(f'100*sum(rate(container_pressure_{res}_waiting_seconds_total{{{nsel}}}[1m]))')
        r[f"psi_{res}_full_pct"] = avg_over(f'100*sum(rate(container_pressure_{res}_stalled_seconds_total{{{nsel}}}[1m]))')
    wsel = 'namespace="benchmark-workloads",pod=~"benchmark-worker.*"'
    r["worker_cpu_cores"] = avg_over(f'sum(rate(container_cpu_usage_seconds_total{{{wsel},container!=""}}[1m]))')
    r["worker_mem_gib_max"] = (max_over(f'sum(container_memory_working_set_bytes{{{wsel},container!=""}})') or 0) / 2**30
    r["worker_throttle_pct"] = avg_over(
        f'100*sum(rate(container_cpu_cfs_throttled_periods_total{{{wsel}}}[1m]))/sum(rate(container_cpu_cfs_periods_total{{{wsel}}}[1m]))')
    r["atelet_cpu_cores"] = avg_over('sum(rate(container_cpu_usage_seconds_total{namespace="ate-system",pod=~"atelet.*",container!=""}[1m]))')
    r["loadgen_cpu_cores"] = avg_over(f'sum(rate(container_cpu_usage_seconds_total{{{sel},container="runner"}}[1m]))')
    r["loadgen_throttle_pct"] = avg_over(
        f'100*sum(rate(container_cpu_cfs_throttled_periods_total{{{sel}}}[1m]))/sum(rate(container_cpu_cfs_periods_total{{{sel}}}[1m]))')
    r["loadgen_mem_gib_max"] = (max_over(f'sum(container_memory_working_set_bytes{{{sel},container="runner"}})') or 0) / 2**30
    r["router_cpu_cores"] = avg_over('sum(rate(container_cpu_usage_seconds_total{namespace="ate-system",pod=~"atenet-router.*",container!=""}[1m]))')
    r["apiserver_cpu_cores"] = avg_over('sum(rate(container_cpu_usage_seconds_total{namespace="ate-system",pod=~"ate-api-server.*",container!=""}[1m]))')
    r["postgres_cpu_cores"] = avg_over('sum(rate(container_cpu_usage_seconds_total{namespace="ate-system",pod=~"postgres.*",container!=""}[1m]))')
    for k_, v in list(r.items()):
        if isinstance(v, float):
            r[k_] = round(v, 3)
    return r


def agent_verdict(r):
    """No latency bar: a step is clean when failures < FINAL_HOLD_MAX_FAIL and >= 98% of users hold a running actor."""
    run = r.get("db_running_assigned_last")
    shortfall = None if run is None else round(max(0.0, 1 - run / max(1, r["users_target"])), 4)
    clean = r["fail_rate"] < FINAL_HOLD_MAX_FAIL and shortfall is not None and shortfall < CLEAN_MAX_SHORTFALL
    return dict(placement_shortfall=shortfall, agent_clean=clean,
                agent_abort=r["fail_rate"] > ABORT_FAIL_RATE or (shortfall or 0) > ABORT_FAIL_RATE)


def attach_db(r, db_samples, a, b):
    near = [s for s in db_samples if a - 60 <= s.get("t", 0) <= b + 60 and "registered" in s]
    # Only samples inside the step: after the last step the runner deletes its actors, and a sample
    # taken then would read 0 running.
    inside = [s for s in db_samples if a <= s.get("t", 0) <= b and "registered" in s] or near
    if near:
        r["db_registered_max"] = max(s["registered_sb"] for s in near)
        r["db_running_assigned_max"] = max(s["running_assigned"] for s in near)
        r["db_running_assigned_min"] = min(s["running_assigned"] for s in inside)
        r["db_running_assigned_last"] = inside[-1]["running_assigned"]


PROBE_HEADLINE = {  # summary key -> stat
    "arp_cache_entries": "max", "ndisc_cache_entries": "max", "conntrack_count": "max", "host_ip4_neigh_lines": "max",
    "gc_thresh3": "last", "max_user_watches": "last", "max_user_instances": "last",
    "ino_watch_total": "max", "ino_inst_total": "max", "ino_watch_kubelet": "max", "ino_inst_kubelet": "max",
    "mem_MemAvailable_kb": "min", "mem_AnonPages_kb": "max", "mem_Shmem_kb": "max", "mem_Slab_kb": "max",
    "mem_SUnreclaim_kb": "max", "mem_Percpu_kb": "max", "mem_PageTables_kb": "max", "mem_KernelStack_kb": "max",
    "cgroup_nr_descendants": "max", "cgroup_nr_dying_descendants": "max", "sandbox_cgroups": "max",
    "sandbox_mem_current_sum": "max", "wpods_mem_current_total": "max", "kernel_tasks_total": "max",
    "threads_exe": "last", "threads_runsc_fd_parking": "last", "threads_ateworker": "max", "threads_atelet": "last",
    "threads_kubelet": "last", "procs_exe": "last", "workers_fds_total": "max", "workers_mountinfo_max": "max",
    "workers_run_netns_total": "max", "files_allocated": "max", "psi_cpu_some_avg10": "max",
    "psi_memory_some_avg10": "max", "psi_memory_full_avg10": "max", "psi_io_some_avg10": "max", "sample_cost_s": "max",
}


def attach_probe(e, r, tag):
    p = e.out / f"probe_{tag}.txt"
    if not p.exists():
        return
    summ, header = None, None
    for line in p.read_text(errors="replace").splitlines():
        if line.startswith("{"):
            try:
                o = json.loads(line)
            except ValueError:
                continue
            if o.get("type") == "summary":
                summ = o
            elif o.get("type") == "header":
                header = o
    if not summ:
        r["probe"] = {"file": p.name, "error": "no summary line"}
        return
    keys = summ.get("keys", {})
    pr = {"file": p.name, "samples": summ.get("samples")}
    for k_, stat in PROBE_HEADLINE.items():
        if k_ in keys:
            pr[k_] = keys[k_][stat]
    pr["cores"] = summ.get("cores", {})
    pr["deltas"] = {k_: v for k_, v in summ.get("deltas", {}).items() if v}
    if header:
        pr["sysctls"] = {k_[7:]: v for k_, v in header.items() if k_.startswith("sysctl_")}
    run = r.get("db_running_assigned_last") or r.get("running_atelet_avg")
    if run and pr.get("sandbox_mem_current_sum"):
        pr["sandbox_mem_mib_per_actor"] = round(pr["sandbox_mem_current_sum"] / 2**20 / run, 1)
    r["probe"] = pr


def agent_quick_eval(e, job, p, a, b, samples):
    """Live check of one finished step: failure rate (Prometheus + log-only failures) and placement shortfall."""
    d = max(1, int(b - a))
    reqs = agent_requests(e, f'pod=~"{job}.*"', d, int(b))
    since = dt.datetime.fromtimestamp(a - 5, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    logs = e.k("logs", f"job/{job}", "-n", "benchmarking", f"--since-time={since}", check=False).stdout or ""
    events, _ = parse_runner_events(logs)
    ev = summarize_events(events, a, b)
    fr, ok, fail, extra = agent_fail_rate(reqs, ev)
    run = next((s["running_assigned"] for s in reversed(samples)
                if "running_assigned" in s and s.get("t", 0) <= b), None)
    shortfall = None if run is None else max(0.0, 1 - run / max(1, p["users"]))
    return dict(fail_rate=fr, ok=ok, fail=fail, log_only_fail=extra, running=run, shortfall=shortfall,
                top=list(ev["log_by_msg_class"].items())[:3])


def make_agent_monitor(e, plan, label):
    """run_job monitor: after each step's window (+30 s for scrape lag) evaluate it. Stop the ladder when the
    failure rate or placement shortfall exceeds ABORT_FAIL_RATE; before a 'hold_ext' step, stop unless the
    top step is clean (failures < FINAL_HOLD_MAX_FAIL and shortfall < CLEAN_MAX_SHORTFALL)."""
    st = {"t0": None, "next": 0}

    def mon(job, t_submit, samples):
        now = time.time()
        if st["t0"] is None:
            series = e.qrange(f'sum(locust_users{{job="boomer-worker",pod=~"{job}.*"}})', t_submit, now, 5)
            firsts = [int(float(t)) for s in series for t, v in s["values"] if float(v) > 0]
            if not firsts:
                return None
            st["t0"] = min(firsts) - 5
            log(f"{label}: ladder t0 {st['t0']} ({int(st['t0'] - t_submit)} s after submit)")
        while st["next"] < len(plan):
            i = st["next"]
            p = plan[i]
            s = st["t0"] + p["start_offset"]
            a, b = s + p["ramp_s"] + p.get("settle_s", SETTLE_S), s + p["hold_s"]
            if now < b + 30:
                return None
            st["next"] += 1
            q = agent_quick_eval(e, job, p, a, b, samples)
            log(f"{label} live users={p['users']} ({p.get('phase')}): fail_rate={q['fail_rate']} ok={q['ok']} "
                f"fail={q['fail']} log_only_fail={q['log_only_fail']} running={q['running']} "
                f"shortfall={None if q['shortfall'] is None else round(q['shortfall'], 3)} top={q['top']}")
            nxt = plan[i + 1] if i + 1 < len(plan) else None
            if nxt is None:
                return None
            sf = q["shortfall"] or 0.0
            if nxt.get("phase") == "hold_ext":
                if q["fail_rate"] >= FINAL_HOLD_MAX_FAIL or sf >= CLEAN_MAX_SHORTFALL:
                    return (f"top step {p['users']} not clean (fail_rate {q['fail_rate']}, shortfall {round(sf, 3)}); "
                            f"skipping the extended hold")
            elif q["fail_rate"] > ABORT_FAIL_RATE or sf > ABORT_FAIL_RATE:
                return f"step {p['users']} fail_rate {q['fail_rate']} / shortfall {round(sf, 3)} > {ABORT_FAIL_RATE}; stop ramping"
        return None
    return mon


def analyze_test_agent(e, test, jobinfo, plan, node, db_samples):
    wins, pts = step_windows(e, jobinfo, plan)
    logs_p = e.out / f"{test}_runner_logs.txt"
    events, first_t = parse_runner_events(logs_p.read_text(errors="replace") if logs_p.exists() else "")
    rows = []
    for w in wins:
        if w["end"] > jobinfo["finished"]:
            w["end"] = jobinfo["finished"]
        if w["end"] - w["start"] < 60:
            continue
        r = analyze_step_agent(e, w, jobinfo["job"] + ".*", node, events, first_t)
        attach_db(r, db_samples, w["start"], w["end"])
        r.update(agent_verdict(r))
        attach_probe(e, r, f"{test}_{w['users']}u" + ("_hold" if w.get("phase") == "hold_ext" else ""))
        rows.append(r)
        pr = r.get("probe", {})
        log(f"{test} users={r['users_target']} ({r['phase']}): running db={r.get('db_running_assigned_last')} "
            f"atelet~{r['running_atelet_avg']} fail_rate={r['fail_rate']} fails={r['failures_by_name']} "
            f"log_classes={r['log_by_class']} create_p95={r['CreateActor_p95_s']} step_p95={r['Step_ALL_p95_s']} "
            f"cpu={r['node_cpu_cores_avg']} node_mem={r['node_mem_gib_max']:.1f}GiB actor_mem={r['actor_mem_gib_max']:.1f}GiB "
            f"psi_mem={r['psi_memory_some_pct']} arp={pr.get('arp_cache_entries')} ino_watch={pr.get('ino_watch_total')} "
            f"clean={r['agent_clean']}")
    raw = {}
    sel = f'job="boomer-worker",pod=~"{jobinfo["job"]}.*"'  # dedupe (2x)
    for key, expr in {
        "users": f'sum(locust_users{{{sel}}})',
        "ok_rate_by_op": f'sum by (name) (rate(locust_requests_total{{{sel},status="success"}}[1m]))',
        "fail_rate_by_op": f'sum by (name) (rate(locust_requests_total{{{sel},status="failure"}}[1m]))',
        "running_atelet": 'sum(ate_actor_stats_sampled_actors)',
        "actor_mem": 'sum(ate_actor_stats_memory_working_set_bytes)',
        "node_cpu": f'sum(rate(container_cpu_usage_seconds_total{{container="node",kubernetes_io_hostname="{node}"}}[1m]))',
        "node_mem": f'sum(container_memory_working_set_bytes{{container="node",kubernetes_io_hostname="{node}"}})',
        "psi_io": f'100*sum(rate(container_pressure_io_waiting_seconds_total{{container="node",kubernetes_io_hostname="{node}"}}[1m]))',
        "psi_cpu": f'100*sum(rate(container_pressure_cpu_waiting_seconds_total{{container="node",kubernetes_io_hostname="{node}"}}[1m]))',
        "psi_mem": f'100*sum(rate(container_pressure_memory_waiting_seconds_total{{container="node",kubernetes_io_hostname="{node}"}}[1m]))',
    }.items():
        raw[key] = e.qrange(expr, jobinfo["submitted"], jobinfo["finished"], 15)
    (e.out / f"{test}_prom_raw.json").write_text(json.dumps(raw))
    (e.out / f"{test}_steps.json").write_text(json.dumps(rows, indent=1))
    return rows


# ---------------------------------------------------------------- main flow


def run_case(case, run_id, skip_deploy=False, start_from="T1", knee=None, knee_label="base", density=None, agent=None):
    global LOG
    out = RESULTS_ROOT / run_id / case
    out.mkdir(parents=True, exist_ok=True)
    LOG = open(out / "driver.log", "a")
    e = Env(case, out)
    summary = {"case": case, "run_id": run_id, "commit": COMMIT, "machine": e.c["machine"],
               "sandbox": e.c["sandbox"], "started": int(time.time()), "tests": {}}

    def save():
        if agent:
            fname = f"summary_A_{knee_label}.json"
        else:
            fname = f"summary_{'D' if density else 'K'}_{knee_label}.json" if (knee or density) else "summary.json"
        (out / fname).write_text(json.dumps(summary, indent=1))
        subprocess.run(["gcloud", "storage", "cp", "-r", str(out),
                        f"gs://{e.c.get('bucket', BUCKET)}/sweperf-stress/{run_id}/driver/"],
                       stdout=LOG, stderr=subprocess.STDOUT)

    try:
        node = target_node(e)
        summary["node"] = node
        try:
            zone = e.k("get", "node", node, "-o", "jsonpath={.metadata.labels.topology\\.kubernetes\\.io/zone}").stdout.strip()
            e.instance_id = subprocess.run(["gcloud", "compute", "instances", "describe", node, "--zone", zone,
                                            "--project", "pmandewalkar-gke-dev", "--format=value(id)"],
                                           capture_output=True, text=True).stdout.strip()
            disk = subprocess.run(["gcloud", "compute", "disks", "describe", node, "--zone", zone, "--project",
                                   "pmandewalkar-gke-dev", "--format=json(type,sizeGb,provisionedIops,provisionedThroughput)"],
                                  capture_output=True, text=True).stdout
            summary["boot_disk"] = json.loads(disk or "{}")
            log(f"node {node} instance {e.instance_id} disk {summary['boot_disk']}")
        except Exception as ex:
            log(f"warn: could not resolve instance/disk: {ex}")
        log(f"=== case {case} on {node} ({e.c['machine']}, {e.c['sandbox']}) run {run_id}")
        # Quiesce the old interactive locust deployment (not part of the benchmark).
        for d in ("locust", "locust-worker"):
            e.k("scale", "deploy", d, "-n", "benchmarking", "--replicas=0", check=False)
        e.k("apply", "-f", str(e.clone / "benchmarking/monitoring.yaml"), check=False)
        e.k("apply", "-f", str(e.clone / "benchmarking/telemetry/meter.yaml"), check=False)
        # Upstream layout: sandbox nodes carry ate.dev/sandboxClass=shared:NoSchedule,
        # which the WorkerPool template and atelet tolerate and the control plane
        # (postgres, ateapi...) does not. Without it postgres can land on a metal
        # node, where pd-balanced volumes cannot attach.
        e.k("taint", "nodes", "-l", e.c["node_label"], "ate.dev/sandboxClass=shared:NoSchedule", "--overwrite")
        if not skip_deploy:
            teardown(e)
            deploy(e, node)
        elif knee or density or agent:
            # Chained drill on the same install: clear leftover benchmark actors instead of reinstalling.
            if not reset_actors(e):
                log("reset: leftovers remain; full reinstall")
                teardown(e)
                deploy(e, node)
            else:
                ensure_overcommit(e, node)
        if start_from != "T1":
            prev = json.loads((out / "summary.json").read_text())
            summary["tests"] = prev.get("tests", {})
            if "derived" in prev:
                summary["derived"] = prev["derived"]
            else:
                rows = json.loads((out / "T1_steps.json").read_text())
                b = best(rows, "resumes_per_s")
                low = rows[0]
                cw = sum(filter(None, [low.get("ResumeActor_p50_s"), low.get("cycle_cel_mean_s"),
                                       low.get("SuspendActor_p50_s")])) or 10.0
                summary["derived"] = dict(t1_max_resumes_per_s=(b or {}).get("resumes_per_s") or 1.0,
                                          low_load_cycle_wall_s=round(cw, 2))
            log(f"resume: derived {summary['derived']}")
            summary["resumed_from"] = start_from
            if not reset_actors(e):
                teardown(e); deploy(e, node)
        gate(e, node)

        if agent:
            # Idle-active density: N users = N resident actors (--lifecycle-mode none), upward-only ladder.
            label = f"A_{knee_label}"
            users = [int(x) for x in agent.split(",")]
            settle = int(max(60, AGENT["phase_in"] + 60))
            spec, total, plan = agent_ladder(users, AGENT["hold"], AGENT["spawn"], settle, AGENT["final_hold"])
            log(f"agentsession plan: users={users} spawn={AGENT['spawn']}/s settle={settle}s hold={AGENT['hold']}s "
                f"final_hold={AGENT['final_hold']}s script={AGENT['script']} think={AGENT['think_scale']} "
                f"lifecycle={LIFECYCLE_MODE or 'none'} actor={ACTOR_CPU or '1'}/{ACTOR_MEMORY or '256Mi'} "
                f"workers={WORKERS} total {total}s; abort>{ABORT_FAIL_RATE}")
            t_sub = time.time()
            if DENSITY_PROBE:
                probes = whole_step_probes(plan, label, t_sub, PROBE_AT or None)
            else:
                probes = [(t_sub + 45 + p_["start_offset"] + p_["ramp_s"] + p_["settle_s"] + 30,
                           f"{label}_{p_['users']}u" + ("_hold" if p_["phase"] == "hold_ext" else ""))
                          for p_ in plan if p_["users"] in PROBE_AT]
            j = run_job(e, label, spec, total, users[0], AGENT["spawn"], 0, 0, run_id, probes=probes, node=node,
                        monitor=make_agent_monitor(e, plan, label))
            rows = analyze_test_agent(e, label, j, plan, node, j["samples"])
            summary["tests"][label] = dict(
                job=j, plan=plan, steps=rows, workload="agentsession", agent=AGENT, lifecycle_mode=LIFECYCLE_MODE or "none",
                actor_cpu=ACTOR_CPU, actor_memory=ACTOR_MEMORY, workers=WORKERS, overcommit=OVERCOMMIT,
                overcommit_mem=OVERCOMMIT_MEM, density_probe=DENSITY_PROBE, abort_fail_rate=ABORT_FAIL_RATE,
                stopped=j.get("stop_reason"))
            summary["finished"] = int(time.time())
            save()
            log(f"=== case {case} agentsession drill {label} done ({j['status']}{': ' + j['stop_reason'] if j.get('stop_reason') else ''})")
            return

        if density:
            # Paced fill: users (= actors) are added slowly and each waits a long think time between
            # cycles, so the activation rate stays far below saturation. The question per step is
            # "does the node still hold N actors and wake one normally", not throughput.
            label = f"D_{knee_label}"
            users = [int(x) for x in density.split(",")]
            spawn = DENSITY_SPAWN or (1.0 if e.c["sandbox"] == "microvm" else 2.0)
            spec, total, plan = ladder(users, lambda u: DENSITY_HOLD or (MEASURE_S + 60), spawn=spawn)
            mean_cycle = sum(DENSITY_THINK) / 2
            log(f"density plan: users={users} spawn={spawn}/s think={DENSITY_THINK} "
                f"steady wakes/s at top step ~{users[-1] / mean_cycle:.2f}; total {total}s")
            t_sub = time.time()
            probes = [(t_sub + 45 + p_["start_offset"] + p_["hold_s"] - PROBE_SECS - 30, f"{label}_{p_['users']}u")
                      for p_ in plan if not PROBE_AT or p_["users"] in PROBE_AT]
            if DENSITY_PROBE:
                probes = whole_step_probes(plan, label, t_sub, PROBE_AT or None)
            j = run_job(e, label, spec, total, users[0], spawn, DENSITY_THINK[0], DENSITY_THINK[1], run_id,
                        probes=probes, node=node)
            rows = density_verdict(analyze_test(e, label, j, plan, node, j["samples"]))
            (e.out / f"{label}_steps.json").write_text(json.dumps(rows, indent=1))
            summary["tests"][label] = dict(job=j, plan=plan, steps=rows, think=DENSITY_THINK, spawn=spawn)
            summary["finished"] = int(time.time())
            save()
            log(f"=== case {case} density drill {label} done")
            return

        if knee:
            label = f"K_{knee_label}"
            users = [int(x) for x in knee.split(",")]
            spec, total, plan = ladder(users, lambda u: MEASURE_S + 60, spawn=5)
            t_sub = time.time()
            probes = []
            for p_ in plan:
                if p_["users"] in PROBE_AT:
                    # ~45 s for the runner pod to start, then into the step's measurement window.
                    probes.append((t_sub + 45 + p_["start_offset"] + p_["ramp_s"] + SETTLE_S + 30,
                                   f"{label}_{p_['users']}u"))
            if DENSITY_PROBE:
                probes = whole_step_probes(plan, label, t_sub, PROBE_AT or None)
            j = run_job(e, label, spec, total, users[0], 5, 0.0, 0.5, run_id, probes=probes, node=node)
            rows = analyze_test(e, label, j, plan, node, j["samples"])
            summary["tests"][label] = dict(job=j, plan=plan, steps=rows)
            summary["finished"] = int(time.time())
            save()
            log(f"=== case {case} knee drill {label} done")
            return

        if start_from == "T1":
          # ---- T1: closed loop
          spec, total, plan = ladder(T1_USERS, lambda u: MEASURE_S, spawn=5)
          j = run_job(e, "T1", spec, total, T1_USERS[0], 5, 0.0, 0.5, run_id)
          rows = analyze_test(e, "T1", j, plan, node, j["samples"])
          summary["tests"]["T1"] = dict(job=j, plan=plan, steps=rows)
          save()
          b = best(rows, "resumes_per_s")
          t1_max = (b or {}).get("resumes_per_s") or 1.0
          low = rows[0] if rows else {}
          cycle_wall = sum(filter(None, [low.get("ResumeActor_p50_s"), low.get("cycle_cel_mean_s"),
                                         low.get("SuspendActor_p50_s")])) or 10.0
          summary["derived"] = dict(t1_max_resumes_per_s=t1_max, low_load_cycle_wall_s=round(cycle_wall, 2))
          log(f"derived: T1 max {t1_max}/s; low-load cycle wall {cycle_wall:.1f}s")
          save()
          if not reset_actors(e):
              log("reset: too many leftovers; full reinstall")
              teardown(e); deploy(e, node); gate(e, node)

        t1_max = summary["derived"]["t1_max_resumes_per_s"]
        cycle_wall = summary["derived"]["low_load_cycle_wall_s"]
        # ---- T2: 10% duty cycle (think = 9 x cycle wall)
        think = 9 * cycle_wall
        spawn2 = 5
        # Participating-actor capacity estimate at 10% duty: N* = T1max x (cycle + think).
        n_star = t1_max * (cycle_wall + think)
        t2_users = sorted({max(10, int(round(n_star * m / 10.0)) * 10) for m in T2_MULT})
        summary["derived"]["t2_n_star"] = round(n_star, 1)
        summary["derived"]["t2_users"] = t2_users
        log(f"T2: think {think:.1f}s, N* {n_star:.0f}, users {t2_users}")
        spec, total, plan = ladder(t2_users, lambda u: MEASURE_S, spawn=spawn2)
        j = run_job(e, "T2", spec, total, t2_users[0], spawn2, round(0.8 * think, 1), round(1.2 * think, 1), run_id)
        rows = analyze_test(e, "T2", j, plan, node, j["samples"])
        summary["tests"]["T2"] = dict(job=j, plan=plan, steps=rows, think_s=think)
        save()
        if not reset_actors(e):
            log("reset: too many leftovers; full reinstall")
            teardown(e); deploy(e, node); gate(e, node)

        # ---- T2b: long think density, spawn at half of T1 capacity, time-budgeted
        spawn3 = max(1, min(10, int(0.5 * t1_max)))
        users = []
        for u in T2B_USERS:
            _, tot, _ = ladder(users + [u], lambda x: MEASURE_S, spawn3)
            if tot > T2B_BUDGET_S:
                log(f"T2b: step {u} exceeds the {T2B_BUDGET_S//60} min budget at spawn {spawn3}/s; ladder ends at {users[-1] if users else 'none'}")
                break
            users.append(u)
        spec, total, plan = ladder(users, lambda u: MEASURE_S, spawn=spawn3)
        j = run_job(e, "T2b", spec, total, users[0], spawn3, 1500, 2100, run_id)
        rows = analyze_test(e, "T2b", j, plan, node, j["samples"])
        summary["tests"]["T2b"] = dict(job=j, plan=plan, steps=rows, spawn=spawn3,
                                       budget_limited=len(users) < len(T2B_USERS))
        summary["finished"] = int(time.time())
        save()
        log(f"=== case {case} done")
    except Exception as ex:
        summary["error"] = str(ex)
        log(f"!!! case {case} aborted: {ex}")
        save()
        raise
    finally:
        e.prom_stop()


def dry_run(a):
    """Print what run_case would do, without a cluster (no kubectl/gcloud; reads the clone's runner template)."""
    import shlex
    import tempfile

    class DryEnv:
        pass
    e = DryEnv()
    e.name, e.c = a.case, CASES[a.case]
    e.clone, e.out = Path(e.c["clone"]), Path(tempfile.mkdtemp(prefix="bench-dry-"))
    node = "<metal-node>"
    print(f"### bench_driver dry run: case={a.case} run_id={a.run_id} workload={WORKLOAD} label={a.knee_label}")
    print(f"# results -> {RESULTS_ROOT / a.run_id / a.case}/   runner image {LOCUST_IMAGE}  tag {COMMIT}")
    if a.skip_deploy:
        print("# --skip-deploy: reset leftover benchmark actors "
              f"(prefix {ACTOR_PREFIX!r}); ensure_overcommit re-patches worker limits if the cpu limit differs")
    else:
        print("# teardown:")
        for c in (deploy_sh("--delete"), ["hack/install-microvm-deps.sh", "--delete"], ["hack/install-ate.sh", "delete", "ate-system"]):
            print("  $ " + shlex.join(c))
        print("  $ kubectl delete <all *.ate.dev CRDs>")
        print("# deploy:")
        print("  $ " + shlex.join(["hack/install-ate.sh", "deploy", "ate-system", f"--otlp-endpoint={METER}"]))
        if WORKLOAD == "agentsession":
            print(f"  (env BENCH_WORKLOAD_TEMPLATES={AGENT['templates']} -> deploy.sh's WORKLOAD_TEMPLATES after .ate-dev-env.sh)")
        print("  $ " + shlex.join(deploy_sh("--deploy", "--worker-count", str(WORKERS), "--sandbox-class", e.c["sandbox"],
                                         "--wait-timeout", "1500", *workload_deploy_args())))
    tmpl = {"nodeSelector": {"kubernetes.io/hostname": node}}
    if OVERCOMMIT > 0:
        c = DRY_ALLOC["cpu"]
        cpu_m = int(c[:-1]) if c.endswith("m") else int(float(c) * 1000)
        mem_ki = int(DRY_ALLOC["memory"][:-2])
        tmpl["resources"] = worker_resources(cpu_m, mem_ki)
        lim = tmpl["resources"]["limits"]
        lcpu, lmem = int(lim["cpu"][:-1]) / 1000, int(lim["memory"][:-2]) / 2**20
        print(f"# worker limits per worker (assumed allocatable {DRY_ALLOC}): {lim} = {lcpu:.0f} cores, {lmem:.0f} GiB")
        if WORKLOAD == "agentsession":
            def q2m(s):
                return float(s[:-1]) if s.endswith("m") else float(s) * 1000
            def q2mi(s):
                return float(s[:-2]) * (1024 if s.endswith("Gi") else 1) if s[-2:] in ("Mi", "Gi") else float(s) / 2**20
            per_cpu, per_mem = lcpu * 1000 / q2m(ACTOR_CPU), lmem * 1024 / q2mi(ACTOR_MEMORY)
            cap = min(per_cpu, per_mem, 1000)
            print(f"# booking per worker at {ACTOR_CPU}/{ACTOR_MEMORY}: cpu {per_cpu:.0f}, memory {per_mem:.0f}, "
                  f"--max-actors 1000 -> {cap:.0f} actors/worker x {WORKERS} = {cap * WORKERS:.0f}")
    print(f"  $ kubectl patch workerpool {WORKER_POOL} -n benchmark-workloads --type=merge -p '{json.dumps({'spec': {'template': tmpl}})}'")
    if WORKLOAD == "agentsession":
        users = [int(x) for x in a.agentsession_users.split(",")]
        settle = int(max(60, AGENT["phase_in"] + 60))
        spec, total, plan = agent_ladder(users, AGENT["hold"], AGENT["spawn"], settle, AGENT["final_hold"])
        label, spawn, mn, mx = f"A_{a.knee_label}", AGENT["spawn"], 0, 0
    elif a.knee:
        users = [int(x) for x in a.knee.split(",")]
        spec, total, plan = ladder(users, lambda u: MEASURE_S + 60, spawn=5)
        label, spawn, mn, mx = f"K_{a.knee_label}", 5, 0.0, 0.5
    elif a.density:
        users = [int(x) for x in a.density.split(",")]
        spawn = DENSITY_SPAWN or (1.0 if e.c["sandbox"] == "microvm" else 2.0)
        spec, total, plan = ladder(users, lambda u: DENSITY_HOLD or (MEASURE_S + 60), spawn=spawn)
        label, mn, mx = f"D_{a.knee_label}", DENSITY_THINK[0], DENSITY_THINK[1]
    else:
        print("# (T1/T2/T2b flow: not rendered in dry run)")
        return
    print(f"# ladder {label}: --ladder {spec}  (total {total}s = {total / 60:.1f} min)")
    for p_ in plan:
        print(f"#   +{p_['start_offset']:>6}s users={p_['users']:>5} ramp={p_['ramp_s']}s settle={p_.get('settle_s', SETTLE_S)}s "
              f"hold_total={p_['hold_s']}s phase={p_.get('phase', 'ramp')}")
    if WORKLOAD == "agentsession":
        print(f"# live monitor: stop ramping if a step's fail_rate or placement shortfall > {ABORT_FAIL_RATE}; "
              + (f"extended hold kept only if top step fail_rate < {FINAL_HOLD_MAX_FAIL} and shortfall < {CLEAN_MAX_SHORTFALL}"
                 if AGENT["final_hold"] else "no extended hold"))
    job, manifest = render_job(e, label, spec, total, users[0], spawn, mn, mx, a.run_id)
    jobdoc = [d for d in yaml.safe_load_all(manifest) if d and d.get("kind") == "Job"][0]
    print(f"# runner Job {job} (RBAC/namespace docs from the template omitted):")
    print(yaml.safe_dump(jobdoc, sort_keys=False))
    try:  # what runner.py would pass boomer as --config-json (clone's own parser; no bytecode written)
        import importlib.util
        sys.dont_write_bytecode = True
        spc = importlib.util.spec_from_file_location("bc", e.clone / "benchmarking/locust/common/boomer_config.py")
        bc = importlib.util.module_from_spec(spc)
        spc.loader.exec_module(bc)
        print("# boomer --config-json: " + bc.build_config_json(runner_load_args(spec, spawn, mn, mx)))
    except Exception as ex:
        print(f"# (could not render boomer config json: {ex})")
    if DENSITY_PROBE:
        probes = whole_step_probes(plan, label, 0, PROBE_AT or None)
        print("# density probes (offset from submit, tag, secs):")
        for pt in probes:
            print(f"#   +{int(pt[0])}s {pt[1]} {pt[2]}s")
        if probes:
            name, m = probe_manifest(node, probes[0][1], probes[0][2], True)
            docs = list(yaml.safe_load_all(m))
            docs[0]["data"]["probe.sh"] = f"<{len(docs[0]['data']['probe.sh'])} bytes of {DENSITY_PROBE_SCRIPT.name}>"
            print("# first probe manifest:")
            print(yaml.safe_dump_all(docs, sort_keys=False))
    elif PROBE_AT:
        print(f"# node_probe.sh at users {PROBE_AT} for {PROBE_SECS}s")
    shutil.rmtree(e.out, ignore_errors=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("case", choices=sorted(CASES))
    ap.add_argument("--run-id", default=dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    ap.add_argument("--skip-deploy", action="store_true")
    ap.add_argument("--from", dest="start_from", default="T1", choices=["T1", "T2"])
    ap.add_argument("--knee", default=None, help="comma-separated closed-loop users for a fine-step knee drill")
    ap.add_argument("--knee-label", default="base")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--overcommit", type=float, default=0.0)
    ap.add_argument("--density", default=None, help="comma-separated user (actor) counts for a paced-fill density drill")
    ap.add_argument("--think", default="1500,2100", help="density mode: min,max think seconds between a user's cycles")
    ap.add_argument("--density-spawn", type=float, default=0.0, help="density mode: users added per second")
    ap.add_argument("--density-hold", type=int, default=0, help="density mode: seconds to hold each step after its ramp")
    ap.add_argument("--measure", type=int, default=0, help="override the per-step measurement window (s), default 240")
    ap.add_argument("--image", default="", help="override runner image")
    ap.add_argument("--runner-env", default="", help="KEY=VAL[,KEY=VAL] env for the runner/boomer container")
    ap.add_argument("--overcommit-mem", type=float, default=0.0, help="memory overcommit factor (default: same as --overcommit); 1 = CPU-only overcommit")
    ap.add_argument("--sweperf-template", default="", help="ActorTemplate for sweperf (runner --sweperf-template)")
    ap.add_argument("--upload-budget", default="", help="objectstore-plugin --upload-memory-budget, e.g. 64Gi")
    ap.add_argument("--probe-at", default="", help="comma-separated user counts to run the node probe at")
    ap.add_argument("--probe-secs", type=int, default=150)
    # agentsession (idle-active density)
    ap.add_argument("--workload", default="sweperf", choices=["sweperf", "agentsession"])
    ap.add_argument("--agentsession-users", default=None,
                    help="agentsession: comma-separated, strictly increasing user (= running actor) counts")
    ap.add_argument("--agentsession-script", default="personal-assistant-light")
    ap.add_argument("--agentsession-think-scale", type=float, default=0.053, help="0.053 ~ gigascale 10%% active; never 0")
    ap.add_argument("--agentsession-phase-in", type=float, default=60.0, help="seconds over which session starts spread")
    ap.add_argument("--agentsession-resume-retry-budget", type=float, default=None)
    ap.add_argument("--agentsession-spawn", type=float, default=5.0, help="users added per second on each ramp")
    ap.add_argument("--agentsession-hold", type=int, default=600, help="seconds each step is held after ramp + settle")
    ap.add_argument("--agentsession-final-hold", type=int, default=0,
                    help="extra hold (s) at the top step, kept only if the top step is clean")
    ap.add_argument("--lifecycle-mode", default=None, choices=["sync", "async", "none"],
                    help="runner --lifecycle-mode (agentsession default none; sweperf: not passed unless set)")
    ap.add_argument("--actor-cpu", default=None, help="deploy.sh --actor-cpu (agentsession default 250m)")
    ap.add_argument("--actor-memory", default=None, help="deploy.sh --actor-memory (agentsession default 512Mi)")
    ap.add_argument("--abort-fail-rate", type=float, default=0.5)
    ap.add_argument("--final-hold-max-fail", type=float, default=0.05)
    ap.add_argument("--density-probe", action="store_true",
                    help="run density_probe.sh over each probed step (all steps unless --probe-at) instead of node_probe.sh")
    ap.add_argument("--density-probe-interval", type=int, default=15)
    ap.add_argument("--dry-run", action="store_true", help="print deploy commands, ladder, runner Job and probe manifests; no cluster")
    a = ap.parse_args()
    WORKERS = a.workers
    OVERCOMMIT = a.overcommit
    DENSITY_THINK = tuple(float(x) for x in a.think.split(","))
    DENSITY_SPAWN = a.density_spawn
    DENSITY_HOLD = a.density_hold
    if a.measure:
        MEASURE_S = a.measure
    _oc = Path(__file__).with_name("overcommit_override")
    if a.workload == "agentsession" and _oc.exists():
        sys.exit(f"{_oc} exists and would silently replace --overcommit; remove it for agentsession runs")
    if OVERCOMMIT > 0 and _oc.exists():
        OVERCOMMIT = float(_oc.read_text().strip())
    if a.image:
        LOCUST_IMAGE = a.image
    RUNNER_ENV = dict(kv.split("=", 1) for kv in a.runner_env.split(",") if kv)
    SWEPERF_TEMPLATE = a.sweperf_template
    OVERCOMMIT_MEM = a.overcommit_mem
    UPLOAD_BUDGET = a.upload_budget
    PROBE_AT = [int(x) for x in a.probe_at.split(",") if x]
    PROBE_SECS = a.probe_secs
    WORKLOAD = a.workload
    DENSITY_PROBE = a.density_probe
    DENSITY_PROBE_INTERVAL = a.density_probe_interval
    ABORT_FAIL_RATE = a.abort_fail_rate
    FINAL_HOLD_MAX_FAIL = a.final_hold_max_fail
    DRY_RUN = a.dry_run
    LIFECYCLE_MODE = a.lifecycle_mode or ""
    ACTOR_CPU = a.actor_cpu or ""
    ACTOR_MEMORY = a.actor_memory or ""
    if WORKLOAD == "agentsession":
        if not a.agentsession_users:
            sys.exit("--workload agentsession needs --agentsession-users")
        if a.agentsession_think_scale <= 0:
            sys.exit("--agentsession-think-scale must be > 0 (0 is read as 1.0 by boomer)")
        ACTOR_PREFIX = "agent-"
        LIFECYCLE_MODE = a.lifecycle_mode or "none"
        ACTOR_CPU = a.actor_cpu or "250m"
        ACTOR_MEMORY = a.actor_memory or "512Mi"
        AGENT.update(script=a.agentsession_script, think_scale=a.agentsession_think_scale,
                     phase_in=a.agentsession_phase_in, resume_retry_budget=a.agentsession_resume_retry_budget,
                     spawn=a.agentsession_spawn, hold=a.agentsession_hold, final_hold=a.agentsession_final_hold)
        if a.image:
            COMMIT = a.image.rsplit(":", 1)[-1]
        agent_ladder([int(x) for x in a.agentsession_users.split(",")], 1, 1, 0)  # validate: strictly increasing
    if DRY_RUN:
        dry_run(a)
        sys.exit(0)
    run_case(a.case, a.run_id, a.skip_deploy, a.start_from, a.knee, a.knee_label, a.density,
             a.agentsession_users if WORKLOAD == "agentsession" else None)
