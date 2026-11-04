#!/bin/sh
# Density probe (footprint.md section 4): run in a privileged hostPID pod on the sandbox node.
# Samples every INTERVAL seconds for DURATION seconds and prints one JSON object per line:
#   {"type":"header",...}   once: host sysctls (host netns), probe config
#   {"type":"sample",...}   every INTERVAL s: flat key/value counters (see keys below)
#   {"type":"summary",...}  once: first/last/min/max per key, cores from cumulative CPU counters, deltas
# Lines starting with "# " are free text (host `ip -s ntable`, worker limits, dmesg tail); parsers skip them.
# Usage: probe.sh DURATION [INTERVAL=15] [INOTIFY_EVERY=4]
# busybox 1.36 only: no `ip ntable` (reads /proc/net/stat/arp_cache in the host netns instead, and tries the
# host's own /sbin/ip via nsenter -m as a bonus), no find -lname, no date %N, no nsenter -C.
D=${1:-600}; IV=${2:-15}; INO_EVERY=${3:-4}
CG=/proc/1/root/sys/fs/cgroup          # host cgroup v2 root (the pod's own /sys/fs/cgroup is a private cgroupns)
T=/tmp/dprobe; rm -rf $T; mkdir -p $T; cd $T || exit 1

NET_SYSCTLS="net/ipv4/neigh/default/gc_thresh1 net/ipv4/neigh/default/gc_thresh2 net/ipv4/neigh/default/gc_thresh3
net/ipv6/neigh/default/gc_thresh1 net/ipv6/neigh/default/gc_thresh2 net/ipv6/neigh/default/gc_thresh3
net/netfilter/nf_conntrack_max net/ipv4/ip_local_port_range net/core/somaxconn"
HOST_SYSCTLS="fs/inotify/max_user_watches fs/inotify/max_user_instances fs/inotify/max_queued_events
kernel/pid_max kernel/threads-max fs/mount-max fs/file-max fs/nr_open user/max_net_namespaces
user/max_inotify_watches user/max_inotify_instances vm/max_map_count kernel/dmesg_restrict"

kv() { printf '%s\t%s\n' "$1" "$2" >> cur; }
rd() { v=$(head -1 "$1" 2>/dev/null | tr '\t' ' '); echo "${v:-NA}"; }
sanitize() { echo "$1" | tr -c 'A-Za-z0-9_.\n-' '_' | tr '.-' '__'; }

# kv file -> one JSON line (numbers bare, everything else a string)
emit_json() {  # $1 = type, file = cur
  awk -F'\t' -v typ="$1" 'BEGIN{printf "{\"type\":\"%s\"", typ}
    { v=$2; gsub(/\\/,"\\\\",v); gsub(/"/,"\\\"",v)
      if ($2 ~ /^-?[0-9]+(\.[0-9]+)?$/) printf ",\"%s\":%s", $1, $2; else printf ",\"%s\":\"%s\"", $1, v }
    END{print "}"}' cur
}

# Read sysctls from the host netns (net.* are per netns; gc_thresh* exist only in init_net) and the rest.
sysctls() {  # $1 = key prefix
  nsenter -t 1 -n sh -c 'for f in '"$(echo $NET_SYSCTLS)"'; do printf "%s\t%s\n" "$f" "$(cat /proc/sys/$f 2>/dev/null | tr "\t" " ")"; done' 2>/dev/null |
    while IFS="$(printf '\t')" read -r f v; do kv "$1$(sanitize "$f")" "${v:-NA}"; done
  for f in $HOST_SYSCTLS; do kv "$1$(sanitize "$f")" "$(rd /proc/sys/$f)"; done
}

# Process-wide pass: per comm group procs/threads/cpu ticks; worker/atelet pids.
group_awk='
function grp(n) {
  if (n == "exe") return "exe"
  if (n ~ /^runsc-fd-park/) return "runsc_fd_parking"
  if (n ~ /^runsc/) return "runsc_other"
  if (n ~ /^ateworker/ || n ~ /^ateom/) return "ateworker"
  if (n == "atelet") return "atelet"
  if (n ~ /^objectstore/) return "objectstore"
  if (n == "kubelet") return "kubelet"
  if (n == "containerd") return "containerd"
  if (n ~ /^containerd-shim/) return "containerd_shim"
  if (n ~ /^fluent-bit/) return "fluent_bit"
  if (n ~ /^cilium/) return "cilium"
  if (n ~ /^kworker/) return "kworker"
  return "other"
}'
proc_pass() {
  ls -d /proc/[0-9]* 2>/dev/null | awk "$group_awk"'
    { d=$0; pid=substr(d,7); name=""; thr=0
      f=d "/status"; while ((getline l < f) > 0) {
        if (l ~ /^Name:/) { sub(/^Name:[ \t]*/,"",l); name=l }
        else if (l ~ /^Threads:/) { sub(/^Threads:[ \t]*/,"",l); thr=l+0; break } }
      close(f)
      if (name == "") next
      tk=0; f=d "/stat"; if ((getline s < f) > 0) { i=0; for (j=length(s); j>0; j--) if (substr(s,j,1)==")") { i=j; break }
        n=split(substr(s,i+2),a," "); tk=a[12]+a[13] }
      close(f)
      g=grp(name); P[g]++; TH[g]+=thr; TK[g]+=tk; tp++; tt+=thr
      if (g=="ateworker" || g=="atelet" || g=="kubelet") print pid, g, thr > "watch_pids"
    }
    END { for (g in P) { printf "procs_%s\t%d\nthreads_%s\t%d\ncpu_ticks_%s\t%d\n", g, P[g], g, TH[g], g, TK[g] }
          printf "procs_total\t%d\nthreads_total_by_status\t%d\n", tp, tt }' >> cur
}

# cgroup dir of a pid as seen from the host cgroup root (reader is in a private cgroupns, so the path is
# relative with ../ components; strip them and fall back to a find by basename).
pid_cgroup() {
  p=$(awk -F: '$1=="0"{print $3}' /proc/$1/cgroup 2>/dev/null | sed 's#^/##; s#^\(\.\./\)*##')
  [ -n "$p" ] && [ -d "$CG/$p" ] && { echo "$CG/$p"; return; }
  b=${p##*/}; [ -n "$b" ] && find $CG -maxdepth 7 -type d -name "$b" 2>/dev/null | head -1
}
pod_of() {  # walk up to the pod cgroup (systemd: *-pod<uid>.slice, cgroupfs: pod<uid>)
  d=$1; while [ -n "$d" ] && [ "$d" != "$CG" ] && [ "$d" != "/" ]; do
    case "${d##*/}" in *-pod*.slice|pod[0-9a-f]*) echo "$d"; return;; esac; d=${d%/*}; done
}
cg_stats() {  # $1 = prefix, $2 = cgroup dir
  [ -d "$2" ] || { kv "$1_unresolved" 1; return; }
  kv "$1_mem_current" "$(rd $2/memory.current)"; kv "$1_mem_max" "$(rd $2/memory.max)"
  kv "$1_pids_current" "$(rd $2/pids.current)"; kv "$1_pids_max" "$(rd $2/pids.max)"
  awk -v p="$1" '$1=="usage_usec"||$1=="nr_throttled"||$1=="throttled_usec"{printf "%s_cpu_%s\t%s\n", p, $1, $2}' $2/cpu.stat 2>/dev/null >> cur
  awk -v p="$1" '$1=="oom"||$1=="oom_kill"||$1=="max"||$1=="high"{printf "%s_memev_%s\t%s\n", p, $1, $2}' $2/memory.events 2>/dev/null >> cur
}

hex_awk='function hex(s,  i,c,v){v=0; s=tolower(s); for(i=1;i<=length(s);i++){c=index("0123456789abcdef",substr(s,i,1))-1; if(c<0) return v; v=v*16+c}; return v}'
neigh_stats() {  # /proc/net/stat/{arp,ndisc}_cache in the host netns: entries (global), table_fulls/forced_gc summed over CPUs
  nsenter -t 1 -n sh -c 'for t in arp_cache ndisc_cache; do echo "TABLE $t"; cat /proc/net/stat/$t 2>/dev/null; done;
    echo "CT $(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null)"; echo "NEIGH4 $(ip -f inet neigh 2>/dev/null | wc -l)"; echo "NEIGH6 $(ip -f inet6 neigh 2>/dev/null | wc -l)"' 2>/dev/null |
  awk "$hex_awk"'
    $1=="TABLE" { t=$2; hdr=1; next }
    $1=="CT" { print "conntrack_count\t" ($2==""?"NA":$2); next }
    $1=="NEIGH4" { print "host_ip4_neigh_lines\t" $2; next }
    $1=="NEIGH6" { print "host_ip6_neigh_lines\t" $2; next }
    hdr==1 { for (i=1;i<=NF;i++) col[t,$i]=i; hdr=0; next }
    { ent[t]=hex($(col[t,"entries"])); tf[t]+=hex($(col[t,"table_fulls"])); fg[t]+=hex($(col[t,"forced_gc_runs"])); seen[t]=1 }
    END { for (t in seen) { printf "%s_entries\t%d\n%s_table_fulls\t%d\n%s_forced_gc_runs\t%d\n", t, ent[t], t, tf[t], t, fg[t] } }' >> cur
}

dmesg_counts() {
  dmesg 2>/dev/null > dm
  kv dmesg_lines "$(wc -l < dm)"
  kv dmesg_neigh_overflow "$(grep -ciE 'neighbou?r table overflow' dm)"
  kv dmesg_inotify "$(grep -ci 'inotify' dm)"
  kv dmesg_out_of_memory "$(grep -c 'Out of memory' dm)"
  kv dmesg_oom_kill "$(grep -ciE 'oom-kill|oom_kill|Killed process' dm)"
  kv dmesg_hung_task "$(grep -c 'blocked for more than' dm)"
}

meminfo() {
  awk '{k=$1; sub(/:$/,"",k)} k~/^(MemTotal|MemFree|MemAvailable|Buffers|Cached|AnonPages|Shmem|Active\(file\)|Inactive\(file\)|Active\(anon\)|Inactive\(anon\)|Slab|SReclaimable|SUnreclaim|Percpu|PageTables|KernelStack|Committed_AS|Mapped|Unevictable)$/ {
    gsub(/[()]/,"_",k); sub(/_$/,"",k); printf "mem_%s_kb\t%s\n", k, $2}' /proc/meminfo >> cur
}

sandbox_cgroups() {
  find $CG -maxdepth 9 -type d \( -name '*-_sandbox' -o -name '*-_ckpt' \) 2>/dev/null > cgs
  grep -- '-_sandbox$' cgs > sbx; kv ckpt_cgroups "$(grep -c -- '-_ckpt$' cgs)"
  awk '{ d=$0; n++
      f=d "/memory.current"; if ((getline v < f) > 0) mc+=v; close(f)
      f=d "/cpu.stat"; while ((getline l < f) > 0) { split(l,a," "); if (a[1]=="usage_usec") cu+=a[2]; else if (a[1]=="nr_throttled") nt+=a[2]; else if (a[1]=="throttled_usec") tu+=a[2] } close(f)
      f=d "/pids.current"; if ((getline v < f) > 0) pc+=v; close(f)
      f=d "/memory.stat"; while ((getline l < f) > 0) { split(l,a," "); if (a[1] ~ /^(anon|file|shmem|kernel|sock|slab|percpu|kernel_stack|pagetables)$/) ms[a[1]]+=a[2] } close(f) }
    END { printf "sandbox_cgroups\t%d\nsandbox_mem_current_sum\t%.0f\nsandbox_cpu_usage_usec_sum\t%.0f\nsandbox_nr_throttled_sum\t%.0f\nsandbox_throttled_usec_sum\t%.0f\nsandbox_pids_sum\t%.0f\n", n, mc, cu, nt, tu, pc
          for (k in ms) printf "sandbox_memstat_%s_sum\t%.0f\n", k, ms[k] }' sbx >> cur
}

# Worker processes: fds, mounts, named netns (per distinct mount ns), pod cgroup stats; atelet pod.
workers() {
  i=0; : > mntns_seen; nfds=0; nmounts_max=0; nns=0
  awk '$2=="ateworker"' watch_pids 2>/dev/null | sort -k3 -rn > wp
  while read -r pid g thr; do
    i=$((i+1)); fds=$(timeout 10 ls /proc/$pid/fd 2>/dev/null | wc -l); mnt=$(timeout 10 cat /proc/$pid/mountinfo 2>/dev/null | wc -l)
    mns=$(readlink /proc/$pid/ns/mnt 2>/dev/null)
    kv "w${i}_pid" "$pid"; kv "w${i}_threads" "$thr"; kv "w${i}_fds" "$fds"; kv "w${i}_mountinfo_lines" "$mnt"
    kv "w${i}_nsfs_mounts" "$(timeout 10 grep -c nsfs /proc/$pid/mountinfo 2>/dev/null)"
    nfds=$((nfds+fds)); [ "$mnt" -gt "$nmounts_max" ] && nmounts_max=$mnt
    if ! grep -qxF "$mns" mntns_seen; then echo "$mns" >> mntns_seen
      n=$(timeout 10 ls /proc/$pid/root/run/netns 2>/dev/null | wc -l); kv "w${i}_run_netns" "$n"; nns=$((nns+n)); fi
  done < wp
  kv workers_procs "$i"; kv workers_fds_total "$nfds"; kv workers_mountinfo_max "$nmounts_max"; kv workers_run_netns_total "$nns"
  kv workers_distinct_mntns "$(wc -l < mntns_seen)"
  # worker pod cgroups (distinct), cached by pid set
  ws=$(awk '{print $1}' wp | sort | tr '\n' ' ')
  if [ "$ws" != "$(cat wpods_key 2>/dev/null)" ]; then
    echo "$ws" > wpods_key; : > wpods
    for pid in $ws; do c=$(pid_cgroup $pid); p=$(pod_of "$c"); [ -n "$p" ] && echo "$p"; done | sort -u > wpods
  fi
  j=0; while read -r p; do j=$((j+1)); cg_stats "wpod$j" "$p"; done < wpods
  kv worker_pods "$j"
  awk -F'\t' '$1 ~ /^wpod[0-9]+_cpu_usage_usec$/{u+=$2} $1 ~ /^wpod[0-9]+_mem_current$/{m+=$2} $1 ~ /^wpod[0-9]+_pids_current$/{pc+=$2}
    END{printf "wpods_cpu_usage_usec_total\t%.0f\nwpods_mem_current_total\t%.0f\nwpods_pids_current_total\t%.0f\n", u, m, pc}' cur >> cur
  ap=$(awk '$2=="atelet"{print $1; exit}' watch_pids 2>/dev/null)
  if [ -n "$ap" ]; then
    [ "$ap" != "$(cat atelet_key 2>/dev/null)" ] && { echo "$ap" > atelet_key; pod_of "$(pid_cgroup $ap)" > atelet_pod; }
    cg_stats ateletpod "$(cat atelet_pod)"
  fi
}

# inotify instances/watches per comm: one ls -l over every /proc/*/fd, then count "inotify wd:" lines in fdinfo.
inotify_pass() {
  ls -l /proc/[0-9]*/fd 2>/dev/null | awk '
    /^\/proc\/[0-9]+\/fd\/?:$/ { split($0,a,"/"); pid=a[3]; next }
    / -> / { tgt=$NF; fd=$(NF-2)
      if (tgt=="anon_inode:inotify") print "/proc/" pid "/fdinfo/" fd > "ino_fds"
      if (pid in W) { if (tgt ~ /^socket:/) S["socket"]++; else if (tgt ~ /^net:/) S["nsfs_net"]++; else if (tgt ~ /^anon_inode:\[pidfd\]/) S["pidfd"]++;
        else if (tgt ~ /^anon_inode:/) S["anon_other"]++; else if (tgt ~ /^pipe:/) S["pipe"]++; else if (tgt ~ /^\//) S["file"]++; else S["other"]++ } }
    BEGIN { while ((getline l < "wp") > 0) { split(l,b," "); W[b[1]]=1 } close("wp") }
    END { for (k in S) printf "workers_fdtype_%s\t%d\n", k, S[k] >> "cur" }'
  [ -f ino_fds ] || : > ino_fds
  awk "$group_awk"'
    { f=$0; split(f,a,"/"); pid=a[3]; w=0; while ((getline l < f) > 0) if (l ~ /^inotify wd:/) w++; close(f)
      if (!(pid in N)) { cf="/proc/" pid "/comm"; c=""; getline c < cf; close(cf); gsub(/[^A-Za-z0-9_.-]/,"_",c); gsub(/[.-]/,"_",c); N[pid]=(c==""?"unknown":c) }
      c=N[pid]; I[c]++; WT[c]+=w; ti++; tw+=w }
    END { for (c in I) printf "ino_inst_%s\t%d\nino_watch_%s\t%d\n", c, I[c], c, WT[c]
          printf "ino_inst_total\t%d\nino_watch_total\t%d\n", ti, tw }' ino_fds >> cur
  rm -f ino_fds
}

psi() {
  for r in cpu memory io; do
    awk -v r=$r '{t=$1; for(i=2;i<=NF;i++){split($i,a,"="); if(a[1]=="avg10"||a[1]=="total") printf "psi_%s_%s_%s\t%s\n", r, t, a[1], a[2]}}' /proc/pressure/$r 2>/dev/null >> cur
  done
}

sample() {  # $1 = 1 to include the inotify/fd-type pass
  : > cur; : > watch_pids
  now=$(date +%s); kv t "$now"; kv elapsed_s "$((now - T0))"; kv n "$N"
  kv gc_thresh3 "$(nsenter -t 1 -n cat /proc/sys/net/ipv4/neigh/default/gc_thresh3 2>/dev/null || echo NA)"
  kv max_user_watches "$(rd /proc/sys/fs/inotify/max_user_watches)"; kv max_user_instances "$(rd /proc/sys/fs/inotify/max_user_instances)"
  neigh_stats; dmesg_counts; meminfo
  awk '{split($4,a,"/"); printf "loadavg1\t%s\nrunnable_tasks\t%s\nkernel_tasks_total\t%s\n", $1, a[1], a[2]}' /proc/loadavg >> cur
  awk '{printf "files_allocated\t%s\nfiles_max\t%s\n", $1, $3}' /proc/sys/fs/file-nr >> cur
  awk '$1=="nr_descendants"||$1=="nr_dying_descendants"{printf "cgroup_%s\t%s\n", $1, $2}' $CG/cgroup.stat 2>/dev/null >> cur
  awk '$1=="usage_usec"||$1=="user_usec"||$1=="system_usec"{printf "root_cpu_%s\t%s\n", $1, $2}' $CG/cpu.stat 2>/dev/null >> cur
  awk '$1=="anon"||$1=="file"||$1=="kernel"||$1=="slab"||$1=="sock"||$1=="shmem"||$1=="percpu"{printf "root_memstat_%s\t%s\n", $1, $2}' $CG/memory.stat 2>/dev/null >> cur
  proc_pass; sandbox_cgroups; workers; psi
  [ "$1" = 1 ] && inotify_pass
  kv sample_cost_s "$(( $(date +%s) - now ))"
}

# ---- header
T0=$(date +%s); N=0; : > wpods; : > wp
: > cur; kv t0 "$T0"; kv duration_s "$D"; kv interval_s "$IV"; kv inotify_every "$INO_EVERY"
kv node "$(nsenter -t 1 -u cat /proc/sys/kernel/hostname 2>/dev/null)"; kv kernel "$(cat /proc/sys/kernel/osrelease)"; kv ncpu "$(grep -c ^processor /proc/cpuinfo)"
sysctls "sysctl_"
emit_json header
echo "# host ip -s ntable (host binary via nsenter -m -n; empty if absent):"
nsenter -t 1 -m -n ip -s ntable show name arp_cache 2>&1 | head -20 | sed 's/^/# /'
for pid in $(ls -d /proc/[0-9]* | while read d; do c=$(cat $d/comm 2>/dev/null); case "$c" in ateworker*|ateom*) echo ${d#/proc/};; esac; done); do
  echo "# limits of $(cat /proc/$pid/comm 2>/dev/null) pid=$pid:"; grep -E 'Max (open files|processes|locked)' /proc/$pid/limits 2>/dev/null | sed 's/^/#   /'
done

# ---- samples
: > samples.kv
END=$((T0 + D))
while :; do
  ts=$(date +%s)
  ino=0; [ $((N % INO_EVERY)) -eq 0 ] && ino=1; [ $((ts + IV)) -ge $END ] && ino=1
  sample $ino
  emit_json sample
  awk -F'\t' -v n=$N '{print n "\t" $1 "\t" $2}' cur >> samples.kv
  N=$((N + 1))
  now=$(date +%s); [ $now -ge $END ] && break
  sl=$((ts + IV - now)); [ $sl -gt 0 ] && sleep $sl
done

# ---- summary: per numeric key first/last/min/max; cores from cumulative CPU counters; deltas of event counters
awk -F'\t' '
  $3 ~ /^-?[0-9]+(\.[0-9]+)?$/ { k=$2; v=$3+0; if (!(k in F)) { F[k]=v; FN[k]=$1; MN[k]=v; MX[k]=v } L[k]=v; LN[k]=$1; if (v<MN[k]) MN[k]=v; if (v>MX[k]) MX[k]=v; if (k=="t") { TT[$1]=v; NS++ } }
  END {
    printf "{\"type\":\"summary\",\"samples\":%d,\"t_first\":%d,\"t_last\":%d", NS, F["t"], L["t"]
    printf ",\"keys\":{"; s=""
    for (k in F) { printf "%s\"%s\":{\"first\":%.15g,\"last\":%.15g,\"min\":%.15g,\"max\":%.15g}", s, k, F[k], L[k], MN[k], MX[k]; s="," }
    printf "},\"cores\":{"; s=""
    for (k in F) if (k ~ /_usec(_sum|_total)?$/ && k !~ /throttled/ || k ~ /^cpu_ticks_/) {
      dt=TT[LN[k]]-TT[FN[k]]; if (dt<=0) continue
      sc=(k ~ /^cpu_ticks_/) ? 100 : 1e6; printf "%s\"%s\":%.3f", s, k, (L[k]-F[k])/sc/dt; s="," }
    printf "},\"deltas\":{"; s=""
    for (k in F) if (k ~ /^dmesg_|_table_fulls$|_forced_gc_runs$|_memev_|_nr_throttled/) { printf "%s\"%s\":%.15g", s, k, L[k]-F[k]; s="," }
    print "}}"
  }' samples.kv
echo "# dmesg tail (matching neighbour/inotify/oom/hung):"
grep -iE 'neighbou?r table overflow|inotify|Out of memory|oom-kill|Killed process|blocked for more than' dm 2>/dev/null | tail -15 | sed 's/^/# /'
echo "# probe end $(date -u +%T) samples=$N"
