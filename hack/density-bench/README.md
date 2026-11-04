# Parnika's density benchmark harness + baseline v2 data

This is a personal harness, not reviewed or upstream. It wraps the upstream sweperf/boomer load generator and adds node probes, step grading, and A/B campaigns.

## Data
- `baseline_v2_steps.csv`: one row per step for all 24 baseline v2 runs (Oct 28–29). That's 159 rows: region, runtime, cell, users, node cores, CPU/IO/mem PSI, cycles/s, resumes/s, Resume/Suspend/Create/Cycle latencies, and live actors.
  - cells: `suspend_resume` (2–14 s think, suspends between cycles), `busy` (0.5–2 s, no suspend), `mostly_waiting` (30–90 s, no suspend), `lpa` (250m/512Mi light assistant)
  - PSI columns are node-level "some"/"full" averaged over the step window (cAdvisor). They're blank where Prometheus dropped out (usually the failing steps).
- `baseline_v2_graded.md`: per-step pass/fail from `v2_analyze.py` (start fail < 0.5%, request fail < 1%, LPA live/DB gap < 2%, no NotReady > 60 s).
- Raw run folders are in `gs://snapshot-substrate-test-pmandewalkar-gke-dev/sweperf-stress/<run_id>/`:
  - `driver/<case>/` has steps, summary, probes, and prom_raw
  - `runs/` has loadgen stats and failures

## Setup used
- Node: one `c3-highcpu-192-metal` per run (192 vCPU, 503 GiB, Hyperdisk Balanced 6k IOPS / 890 MB/s), in 3 clusters (US/EU/z3)
- Build: main `475f49b0` + prototype switches off + histogram buckets + atunnel fix (PR #2953). gVisor 20261026.0, microVM with #3104. *(The run files record tag `5035a6fa`; I'm confirming which SHA was deployed.)*
- Load generator image `7bfc72c6`. sweperf `swebench-astropy-7336`, 2 CPU / 4 GiB template, actor replaced after each task (4 cycles).
- Capacity booking lifted (worker overcommit) so the scheduler doesn't cap first.

## Code (`code/`)
| File | What it does |
|---|---|
| `bench_driver.py` | Deploys workers and the sweperf job, runs the step ladder, scrapes Prometheus per step, launches the node probe, writes `*_steps.json` / `summary_*.json`, uploads to GCS |
| `campaign.py` | Queues of runs: baseline v2 cells, A/Bs (`with_pre(...)` flags: node I/O tuning, NUMA pin, system CPU protect, halt_poll), busy-burn, NUMA |
| `v2_analyze.py` | Grades steps and builds the pass/fail tables |
| `node_probe.sh` | Privileged hostPID probe: per-process/thread CPU, per-pod `cpu.stat` throttling, `/proc/pressure`, D-state kernel stacks |
| `density_probe.sh` | Density probe: sandbox procs, netns/mount counts, memory, PSI avg10 |
| `node_cpu_profiler2.sh` | CPU usage by cgroup: system.slice, kubepods, worker vs actor leaves per pod, atelet. Doesn't read per-cgroup `cpu.pressure` yet |
| `system_cpu_protect_ds.yaml` | `system.slice` cpu.weight 100 → 1,500 (node-agent protection A/B) |
| `numa_pinner_ds.yaml` | Prototype NUMA pinner (cpuset per worker pod) |
| `node_io_tuner_ds.yaml`, `sysctl_tuner_ds.yaml` | Node writeback / sysctl tuning |
| `plot_busyburn.py` | Busy-burn plots |

> **Not runnable as-is.** Project, cluster contexts, buckets, and image paths are hardcoded to my project (`CASES` near the top of `bench_driver.py`; image constants in `campaign.py`). Swap in your own before running.

## Known caveats
- n = 3 per cell (one run per node). Node-to-node variation is about one step.
- The cgroup CPU metric undercounts microVM (101 vs 132 cores vs /proc/stat).
- Node metrics drop out at the highest steps when node agents are starved.
- Node-level PSI on 192 CPUs is a blunt signal. Per-cgroup PSI is the next step (b/581524657).
