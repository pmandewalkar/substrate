#!/bin/sh
# CPU split on the node by cgroup, every INTERVAL s (cumulative usage_usec; diff offline):
#   S <epoch> system.slice <usec>
#   S <epoch> kubepods <usec>
#   W <epoch> <pod-scope-id> worker <usec>        (worker process + its runsc/helper children)
#   W <epoch> <pod-scope-id> actors <usec> <n>    (sum over actor leaves under the same scope)
#   P <epoch> <cgroup-basename> <usec>            (non-worker pods, e.g. atelet)
INTERVAL=${INTERVAL:-10}
DUR=${DUR:-7200}
R=${R:-/proc/1/root/sys/fs/cgroup}
end=$(( $(date +%s) + DUR ))
i=0
u() { awk '/^usage_usec/ {print $2}' "$1/cpu.stat" 2>/dev/null; }
while [ $(date +%s) -lt $end ]; do
  now=$(date +%s)
  echo "S $now system.slice $(u $R/system.slice)"
  echo "S $now kubepods $(u $R/kubepods.slice)"
  for w in $(find $R/kubepods.slice -maxdepth 6 -type d -name worker 2>/dev/null); do
    scope=$(dirname $w)
    id=$(basename $scope | cut -c1-40)
    echo "W $now $id worker $(u $w)"
    tot=0; n=0
    for a in $scope/*/; do
      a=${a%/}; [ "$a" = "$w" ] && continue
      v=$(u $a); [ -n "$v" ] || continue
      tot=$((tot + v)); n=$((n + 1))
    done
    echo "W $now $id actors $tot $n"
  done
  for p in $(find $R/kubepods.slice -maxdepth 3 -type d -name 'kubepods-*pod*.slice' 2>/dev/null); do
    echo "P $now $(basename $p | cut -c1-60) $(u $p)"
  done
  # deep sampling every 3rd interval: per-CPU stat, run queue, thread states by comm, PSI, cpusets
  i=$((i + 1))
  if [ $((i % 3)) -eq 0 ]; then
    H=/proc/1/root/proc
    grep -E '^cpu[0-9]' $H/stat | awk -v now=$now '{print "C", now, $0}'
    grep -E '^procs_(running|blocked)' $H/stat | awk -v now=$now '{print "Q", now, $1, $2}'
    for f in cpu io memory; do echo "PSI $now $f $(head -1 $H/pressure/$f 2>/dev/null)"; done
    cat $H/[0-9]*/task/[0-9]*/stat 2>/dev/null | awk -v now=$now '{c=$0; sub(/^[0-9]+ \(/, "", c); comm=c; sub(/\) .*/, "", comm); st=c; sub(/^.*\) /, "", st); s=substr(st,1,1); gsub(/[0-9]+$/, "N", comm); if (s=="R"||s=="D") n[comm" "s]++} END {for (k in n) if (n[k]>=3) print "T", now, k, n[k]}'
    for w in $(find $R/kubepods.slice -maxdepth 6 -type d -name worker 2>/dev/null | head -2); do echo "CS $now $(cat $(dirname $w)/cpuset.cpus.effective 2>/dev/null)"; done
  fi
  sleep $INTERVAL
done
